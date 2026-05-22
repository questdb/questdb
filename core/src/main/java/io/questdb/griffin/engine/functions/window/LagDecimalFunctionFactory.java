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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.cairo.sql.VirtualRecord;
import io.questdb.cairo.sql.WindowSPI;
import io.questdb.cairo.vm.api.MemoryARW;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimals;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;

public class LagDecimalFunctionFactory extends AbstractWindowFunctionFactory {

    private static final String SIGNATURE = LeadLagWindowFunctionFactoryHelper.LAG_NAME + "(ΞV)";

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
        final int argType = args.get(0).getType();
        final int tag = ColumnType.tagOf(argType);
        return switch (tag) {
            case ColumnType.DECIMAL8 -> LeadLagWindowFunctionFactoryHelper.newInstance(
                    position, args, argPositions, configuration, sqlExecutionContext,
                    (defaultValue) -> {
                        if (!ColumnType.isSameOrBuiltInWideningCast(defaultValue.getType(), argType)) {
                            throw SqlException.$(argPositions.getQuick(2), "default value must be castable to decimal");
                        }
                    },
                    (arg, defaultValueFunc, offset, memory, ignoreNulls) -> new Decimal8LagFunction(arg, defaultValueFunc, offset, memory, ignoreNulls, argType),
                    (partitionByRecord, arg, name, ignoreNulls) -> new Decimal8LeadLagValueCurrentRow(partitionByRecord, arg, name, ignoreNulls, argType),
                    (map, partitionByRecord, partitionBySink, memory, arg, ignoreNulls, defaultValue, offset) -> new Decimal8LagOverPartitionFunction(map, partitionByRecord, partitionBySink, memory, arg, ignoreNulls, defaultValue, offset, argType)
            );
            case ColumnType.DECIMAL16 -> LeadLagWindowFunctionFactoryHelper.newInstance(
                    position, args, argPositions, configuration, sqlExecutionContext,
                    (defaultValue) -> {
                        if (!ColumnType.isSameOrBuiltInWideningCast(defaultValue.getType(), argType)) {
                            throw SqlException.$(argPositions.getQuick(2), "default value must be castable to decimal");
                        }
                    },
                    (arg, defaultValueFunc, offset, memory, ignoreNulls) -> new Decimal16LagFunction(arg, defaultValueFunc, offset, memory, ignoreNulls, argType),
                    (partitionByRecord, arg, name, ignoreNulls) -> new Decimal16LeadLagValueCurrentRow(partitionByRecord, arg, name, ignoreNulls, argType),
                    (map, partitionByRecord, partitionBySink, memory, arg, ignoreNulls, defaultValue, offset) -> new Decimal16LagOverPartitionFunction(map, partitionByRecord, partitionBySink, memory, arg, ignoreNulls, defaultValue, offset, argType)
            );
            case ColumnType.DECIMAL32 -> LeadLagWindowFunctionFactoryHelper.newInstance(
                    position, args, argPositions, configuration, sqlExecutionContext,
                    (defaultValue) -> {
                        if (!ColumnType.isSameOrBuiltInWideningCast(defaultValue.getType(), argType)) {
                            throw SqlException.$(argPositions.getQuick(2), "default value must be castable to decimal");
                        }
                    },
                    (arg, defaultValueFunc, offset, memory, ignoreNulls) -> new Decimal32LagFunction(arg, defaultValueFunc, offset, memory, ignoreNulls, argType),
                    (partitionByRecord, arg, name, ignoreNulls) -> new Decimal32LeadLagValueCurrentRow(partitionByRecord, arg, name, ignoreNulls, argType),
                    (map, partitionByRecord, partitionBySink, memory, arg, ignoreNulls, defaultValue, offset) -> new Decimal32LagOverPartitionFunction(map, partitionByRecord, partitionBySink, memory, arg, ignoreNulls, defaultValue, offset, argType)
            );
            case ColumnType.DECIMAL64 -> LeadLagWindowFunctionFactoryHelper.newInstance(
                    position, args, argPositions, configuration, sqlExecutionContext,
                    (defaultValue) -> {
                        if (!ColumnType.isSameOrBuiltInWideningCast(defaultValue.getType(), argType)) {
                            throw SqlException.$(argPositions.getQuick(2), "default value must be castable to decimal");
                        }
                    },
                    (arg, defaultValueFunc, offset, memory, ignoreNulls) -> new Decimal64LagFunction(arg, defaultValueFunc, offset, memory, ignoreNulls, argType),
                    (partitionByRecord, arg, name, ignoreNulls) -> new Decimal64LeadLagValueCurrentRow(partitionByRecord, arg, name, ignoreNulls, argType),
                    (map, partitionByRecord, partitionBySink, memory, arg, ignoreNulls, defaultValue, offset) -> new Decimal64LagOverPartitionFunction(map, partitionByRecord, partitionBySink, memory, arg, ignoreNulls, defaultValue, offset, argType)
            );
            case ColumnType.DECIMAL128 -> LeadLagWindowFunctionFactoryHelper.newInstance(
                    position, args, argPositions, configuration, sqlExecutionContext,
                    (defaultValue) -> {
                        if (!ColumnType.isSameOrBuiltInWideningCast(defaultValue.getType(), argType)) {
                            throw SqlException.$(argPositions.getQuick(2), "default value must be castable to decimal");
                        }
                    },
                    (arg, defaultValueFunc, offset, memory, ignoreNulls) -> new Decimal128LagFunction(arg, defaultValueFunc, offset, memory, ignoreNulls, argType),
                    (partitionByRecord, arg, name, ignoreNulls) -> new Decimal128LeadLagValueCurrentRow(partitionByRecord, arg, name, ignoreNulls, argType),
                    (map, partitionByRecord, partitionBySink, memory, arg, ignoreNulls, defaultValue, offset) -> new Decimal128LagOverPartitionFunction(map, partitionByRecord, partitionBySink, memory, arg, ignoreNulls, defaultValue, offset, argType)
            );
            case ColumnType.DECIMAL256 -> LeadLagWindowFunctionFactoryHelper.newInstance(
                    position, args, argPositions, configuration, sqlExecutionContext,
                    (defaultValue) -> {
                        if (!ColumnType.isSameOrBuiltInWideningCast(defaultValue.getType(), argType)) {
                            throw SqlException.$(argPositions.getQuick(2), "default value must be castable to decimal");
                        }
                    },
                    (arg, defaultValueFunc, offset, memory, ignoreNulls) -> new Decimal256LagFunction(arg, defaultValueFunc, offset, memory, ignoreNulls, argType),
                    (partitionByRecord, arg, name, ignoreNulls) -> new Decimal256LeadLagValueCurrentRow(partitionByRecord, arg, name, ignoreNulls, argType),
                    (map, partitionByRecord, partitionBySink, memory, arg, ignoreNulls, defaultValue, offset) -> new Decimal256LagOverPartitionFunction(map, partitionByRecord, partitionBySink, memory, arg, ignoreNulls, defaultValue, offset, argType)
            );
            default -> throw SqlException.$(position, "lag is not yet implemented for ").put(ColumnType.nameOf(tag));
        };
    }

    @Override
    protected boolean supportNullsDesc() {
        return true;
    }

    public static class Decimal128LagFunction extends LeadLagWindowFunctionFactoryHelper.BaseLagFunction {

        private final Decimal128 lagValue = new Decimal128();
        private final Decimal128 scratch = new Decimal128();
        private final int type;

        public Decimal128LagFunction(Function arg, Function defaultValueFunc, long offset, MemoryARW memory, boolean ignoreNulls, int type) {
            super(arg, defaultValueFunc, offset, memory, ignoreNulls);
            this.type = type;
            lagValue.ofRawNull();
        }

        @Override
        public boolean computeNext0(Record record) {
            if (count < offset) {
                if (defaultValue == null) {
                    lagValue.ofRawNull();
                } else {
                    defaultValue.getDecimal128(record, lagValue);
                }
            } else {
                buffer.getDecimal128((long) loIdx * Decimal128.BYTES, lagValue);
            }
            arg.getDecimal128(record, scratch);
            boolean respectNulls = !ignoreNulls || !scratch.isNull();
            if (respectNulls) {
                buffer.putDecimal128((long) loIdx * Decimal128.BYTES, scratch.getHigh(), scratch.getLow());
            }
            return respectNulls;
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            sink.copyFrom(lagValue);
        }

        @Override
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            long addr = spi.getAddress(recordOffset, columnIndex);
            Unsafe.putLong(addr, lagValue.getHigh());
            Unsafe.putLong(addr + Long.BYTES, lagValue.getLow());
        }
    }

    static class Decimal128LagOverPartitionFunction extends BasePartitionedWindowFunction {

        private final Function defaultValue;
        private final boolean ignoreNulls;
        private final Decimal128 lagValue = new Decimal128();
        private final MemoryARW memory;
        private final Decimal128 nullScratch = new Decimal128();
        private final long offset;
        private final Decimal128 scratch = new Decimal128();
        private final int type;

        public Decimal128LagOverPartitionFunction(Map map,
                                                  VirtualRecord partitionByRecord,
                                                  RecordSink partitionBySink,
                                                  MemoryARW memory,
                                                  Function arg,
                                                  boolean ignoreNulls,
                                                  Function defaultValue,
                                                  long offset,
                                                  int type) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.memory = memory;
            this.ignoreNulls = ignoreNulls;
            this.defaultValue = defaultValue;
            this.offset = offset;
            this.type = type;
            lagValue.ofRawNull();
            nullScratch.ofRawNull();
        }

        @Override
        public void close() {
            super.close();
            Misc.free(memory);
            Misc.free(defaultValue);
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

            if (mapValue.isNew()) {
                startOffset = memory.appendAddressFor(offset * Decimal128.BYTES) - memory.getPageAddress(0);
                firstIdx = 0;
                for (long i = 0; i < offset; i++) {
                    memory.putDecimal128(startOffset + i * Decimal128.BYTES, nullScratch.getHigh(), nullScratch.getLow());
                }
            } else {
                startOffset = mapValue.getLong(0);
                firstIdx = mapValue.getLong(1);
                count = mapValue.getLong(2);
            }

            arg.getDecimal128(record, scratch);
            if (count < offset) {
                if (defaultValue == null) {
                    lagValue.ofRawNull();
                } else {
                    defaultValue.getDecimal128(record, lagValue);
                }
            } else {
                memory.getDecimal128(startOffset + firstIdx * Decimal128.BYTES, lagValue);
            }
            boolean respectNulls = !ignoreNulls || !scratch.isNull();
            if (respectNulls) {
                memory.putDecimal128(startOffset + firstIdx * Decimal128.BYTES, scratch.getHigh(), scratch.getLow());
                firstIdx++;
                count++;
            }

            mapValue.putLong(0, startOffset);
            mapValue.putLong(1, firstIdx % offset);
            mapValue.putLong(2, count);
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            sink.copyFrom(lagValue);
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
        public int getType() {
            return type;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            super.init(symbolTableSource, executionContext);
            if (defaultValue != null) {
                defaultValue.init(symbolTableSource, executionContext);
            }
        }

        @Override
        public boolean isIgnoreNulls() {
            return ignoreNulls;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            long addr = spi.getAddress(recordOffset, columnIndex);
            Unsafe.putLong(addr, lagValue.getHigh());
            Unsafe.putLong(addr + Long.BYTES, lagValue.getLow());
        }

        @Override
        public void reset() {
            super.reset();
            Misc.free(memory);
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

        @Override
        public void toTop() {
            super.toTop();
            memory.truncate();
        }
    }

    static class Decimal128LeadLagValueCurrentRow extends LeadLagWindowFunctionFactoryHelper.BaseLeadLagCurrentRow {

        private final int type;
        private final Decimal128 value = new Decimal128();

        public Decimal128LeadLagValueCurrentRow(VirtualRecord partitionByRecord, Function arg, String name, boolean ignoreNulls, int type) {
            super(partitionByRecord, arg, name, ignoreNulls);
            this.type = type;
            value.ofRawNull();
        }

        @Override
        public void computeNext(Record record) {
            arg.getDecimal128(record, value);
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            sink.copyFrom(value);
        }

        @Override
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            long addr = spi.getAddress(recordOffset, columnIndex);
            Unsafe.putLong(addr, value.getHigh());
            Unsafe.putLong(addr + Long.BYTES, value.getLow());
        }
    }

    public static class Decimal16LagFunction extends LeadLagWindowFunctionFactoryHelper.BaseLagFunction {

        private final int type;
        private short lagValue;

        public Decimal16LagFunction(Function arg, Function defaultValueFunc, long offset, MemoryARW memory, boolean ignoreNulls, int type) {
            super(arg, defaultValueFunc, offset, memory, ignoreNulls);
            this.type = type;
        }

        @Override
        public boolean computeNext0(Record record) {
            if (count < offset) {
                lagValue = defaultValue == null ? Decimals.DECIMAL16_NULL : defaultValue.getDecimal16(record);
            } else {
                lagValue = buffer.getShort((long) loIdx * Short.BYTES);
            }
            short s = arg.getDecimal16(record);
            boolean respectNulls = !ignoreNulls || s != Decimals.DECIMAL16_NULL;
            if (respectNulls) {
                buffer.putShort((long) loIdx * Short.BYTES, s);
            }
            return respectNulls;
        }

        @Override
        public short getDecimal16(Record rec) {
            return lagValue;
        }

        @Override
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putShort(spi.getAddress(recordOffset, columnIndex), lagValue);
        }
    }

    static class Decimal16LagOverPartitionFunction extends LeadLagWindowFunctionFactoryHelper.BaseLagOverPartitionFunction {

        private final int type;
        private short lagValue;

        public Decimal16LagOverPartitionFunction(Map map,
                                                 VirtualRecord partitionByRecord,
                                                 RecordSink partitionBySink,
                                                 MemoryARW memory,
                                                 Function arg,
                                                 boolean ignoreNulls,
                                                 Function defaultValue,
                                                 long offset,
                                                 int type) {
            super(map, partitionByRecord, partitionBySink, memory, arg, ignoreNulls, defaultValue, offset);
            this.type = type;
        }

        @Override
        public short getDecimal16(Record rec) {
            return lagValue;
        }

        @Override
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putShort(spi.getAddress(recordOffset, columnIndex), lagValue);
        }

        @Override
        protected boolean computeNext0(long count, long offset, long startOffset, long firstIdx, Record record) {
            short s = arg.getDecimal16(record);
            if (count < offset) {
                lagValue = defaultValue == null ? Decimals.DECIMAL16_NULL : defaultValue.getDecimal16(record);
            } else {
                lagValue = memory.getShort(startOffset + firstIdx * Short.BYTES);
            }
            boolean respectNulls = !ignoreNulls || Decimals.DECIMAL16_NULL != s;
            if (respectNulls) {
                memory.putShort(startOffset + firstIdx * Short.BYTES, s);
            }
            return respectNulls;
        }
    }

    static class Decimal16LeadLagValueCurrentRow extends LeadLagWindowFunctionFactoryHelper.BaseLeadLagCurrentRow {

        private final int type;
        private short value;

        public Decimal16LeadLagValueCurrentRow(VirtualRecord partitionByRecord, Function arg, String name, boolean ignoreNulls, int type) {
            super(partitionByRecord, arg, name, ignoreNulls);
            this.type = type;
        }

        @Override
        public void computeNext(Record record) {
            value = arg.getDecimal16(record);
        }

        @Override
        public short getDecimal16(Record rec) {
            return value;
        }

        @Override
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putShort(spi.getAddress(recordOffset, columnIndex), value);
        }
    }

    public static class Decimal256LagFunction extends LeadLagWindowFunctionFactoryHelper.BaseLagFunction {

        private final Decimal256 lagValue = new Decimal256();
        private final Decimal256 scratch = new Decimal256();
        private final int type;

        public Decimal256LagFunction(Function arg, Function defaultValueFunc, long offset, MemoryARW memory, boolean ignoreNulls, int type) {
            super(arg, defaultValueFunc, offset, memory, ignoreNulls);
            this.type = type;
            lagValue.ofRawNull();
        }

        @Override
        public boolean computeNext0(Record record) {
            if (count < offset) {
                if (defaultValue == null) {
                    lagValue.ofRawNull();
                } else {
                    defaultValue.getDecimal256(record, lagValue);
                }
            } else {
                buffer.getDecimal256((long) loIdx * Decimal256.BYTES, lagValue);
            }
            arg.getDecimal256(record, scratch);
            boolean respectNulls = !ignoreNulls || !scratch.isNull();
            if (respectNulls) {
                buffer.putDecimal256((long) loIdx * Decimal256.BYTES, scratch.getHh(), scratch.getHl(), scratch.getLh(), scratch.getLl());
            }
            return respectNulls;
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(lagValue);
        }

        @Override
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            long addr = spi.getAddress(recordOffset, columnIndex);
            Unsafe.putLong(addr, lagValue.getHh());
            Unsafe.putLong(addr + Long.BYTES, lagValue.getHl());
            Unsafe.putLong(addr + 2 * Long.BYTES, lagValue.getLh());
            Unsafe.putLong(addr + 3 * Long.BYTES, lagValue.getLl());
        }
    }

    static class Decimal256LagOverPartitionFunction extends BasePartitionedWindowFunction {

        private final Function defaultValue;
        private final boolean ignoreNulls;
        private final Decimal256 lagValue = new Decimal256();
        private final MemoryARW memory;
        private final Decimal256 nullScratch = new Decimal256();
        private final long offset;
        private final Decimal256 scratch = new Decimal256();
        private final int type;

        public Decimal256LagOverPartitionFunction(Map map,
                                                  VirtualRecord partitionByRecord,
                                                  RecordSink partitionBySink,
                                                  MemoryARW memory,
                                                  Function arg,
                                                  boolean ignoreNulls,
                                                  Function defaultValue,
                                                  long offset,
                                                  int type) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.memory = memory;
            this.ignoreNulls = ignoreNulls;
            this.defaultValue = defaultValue;
            this.offset = offset;
            this.type = type;
            lagValue.ofRawNull();
            nullScratch.ofRawNull();
        }

        @Override
        public void close() {
            super.close();
            Misc.free(memory);
            Misc.free(defaultValue);
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

            if (mapValue.isNew()) {
                startOffset = memory.appendAddressFor(offset * Decimal256.BYTES) - memory.getPageAddress(0);
                firstIdx = 0;
                for (long i = 0; i < offset; i++) {
                    memory.putDecimal256(startOffset + i * Decimal256.BYTES, nullScratch.getHh(), nullScratch.getHl(), nullScratch.getLh(), nullScratch.getLl());
                }
            } else {
                startOffset = mapValue.getLong(0);
                firstIdx = mapValue.getLong(1);
                count = mapValue.getLong(2);
            }

            arg.getDecimal256(record, scratch);
            if (count < offset) {
                if (defaultValue == null) {
                    lagValue.ofRawNull();
                } else {
                    defaultValue.getDecimal256(record, lagValue);
                }
            } else {
                memory.getDecimal256(startOffset + firstIdx * Decimal256.BYTES, lagValue);
            }
            boolean respectNulls = !ignoreNulls || !scratch.isNull();
            if (respectNulls) {
                memory.putDecimal256(startOffset + firstIdx * Decimal256.BYTES, scratch.getHh(), scratch.getHl(), scratch.getLh(), scratch.getLl());
                firstIdx++;
                count++;
            }

            mapValue.putLong(0, startOffset);
            mapValue.putLong(1, firstIdx % offset);
            mapValue.putLong(2, count);
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(lagValue);
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
        public int getType() {
            return type;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            super.init(symbolTableSource, executionContext);
            if (defaultValue != null) {
                defaultValue.init(symbolTableSource, executionContext);
            }
        }

        @Override
        public boolean isIgnoreNulls() {
            return ignoreNulls;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            long addr = spi.getAddress(recordOffset, columnIndex);
            Unsafe.putLong(addr, lagValue.getHh());
            Unsafe.putLong(addr + Long.BYTES, lagValue.getHl());
            Unsafe.putLong(addr + 2 * Long.BYTES, lagValue.getLh());
            Unsafe.putLong(addr + 3 * Long.BYTES, lagValue.getLl());
        }

        @Override
        public void reset() {
            super.reset();
            Misc.free(memory);
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

        @Override
        public void toTop() {
            super.toTop();
            memory.truncate();
        }
    }

    static class Decimal256LeadLagValueCurrentRow extends LeadLagWindowFunctionFactoryHelper.BaseLeadLagCurrentRow {

        private final int type;
        private final Decimal256 value = new Decimal256();

        public Decimal256LeadLagValueCurrentRow(VirtualRecord partitionByRecord, Function arg, String name, boolean ignoreNulls, int type) {
            super(partitionByRecord, arg, name, ignoreNulls);
            this.type = type;
            value.ofRawNull();
        }

        @Override
        public void computeNext(Record record) {
            arg.getDecimal256(record, value);
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(value);
        }

        @Override
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            long addr = spi.getAddress(recordOffset, columnIndex);
            Unsafe.putLong(addr, value.getHh());
            Unsafe.putLong(addr + Long.BYTES, value.getHl());
            Unsafe.putLong(addr + 2 * Long.BYTES, value.getLh());
            Unsafe.putLong(addr + 3 * Long.BYTES, value.getLl());
        }
    }

    public static class Decimal32LagFunction extends LeadLagWindowFunctionFactoryHelper.BaseLagFunction {

        private final int type;
        private int lagValue;

        public Decimal32LagFunction(Function arg, Function defaultValueFunc, long offset, MemoryARW memory, boolean ignoreNulls, int type) {
            super(arg, defaultValueFunc, offset, memory, ignoreNulls);
            this.type = type;
        }

        @Override
        public boolean computeNext0(Record record) {
            if (count < offset) {
                lagValue = defaultValue == null ? Decimals.DECIMAL32_NULL : defaultValue.getDecimal32(record);
            } else {
                lagValue = buffer.getInt((long) loIdx * Integer.BYTES);
            }
            int i = arg.getDecimal32(record);
            boolean respectNulls = !ignoreNulls || i != Decimals.DECIMAL32_NULL;
            if (respectNulls) {
                buffer.putInt((long) loIdx * Integer.BYTES, i);
            }
            return respectNulls;
        }

        @Override
        public int getDecimal32(Record rec) {
            return lagValue;
        }

        @Override
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putInt(spi.getAddress(recordOffset, columnIndex), lagValue);
        }
    }

    static class Decimal32LagOverPartitionFunction extends LeadLagWindowFunctionFactoryHelper.BaseLagOverPartitionFunction {

        private final int type;
        private int lagValue;

        public Decimal32LagOverPartitionFunction(Map map,
                                                 VirtualRecord partitionByRecord,
                                                 RecordSink partitionBySink,
                                                 MemoryARW memory,
                                                 Function arg,
                                                 boolean ignoreNulls,
                                                 Function defaultValue,
                                                 long offset,
                                                 int type) {
            super(map, partitionByRecord, partitionBySink, memory, arg, ignoreNulls, defaultValue, offset);
            this.type = type;
        }

        @Override
        public int getDecimal32(Record rec) {
            return lagValue;
        }

        @Override
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putInt(spi.getAddress(recordOffset, columnIndex), lagValue);
        }

        @Override
        protected boolean computeNext0(long count, long offset, long startOffset, long firstIdx, Record record) {
            int i = arg.getDecimal32(record);
            if (count < offset) {
                lagValue = defaultValue == null ? Decimals.DECIMAL32_NULL : defaultValue.getDecimal32(record);
            } else {
                lagValue = memory.getInt(startOffset + firstIdx * Integer.BYTES);
            }
            boolean respectNulls = !ignoreNulls || Decimals.DECIMAL32_NULL != i;
            if (respectNulls) {
                memory.putInt(startOffset + firstIdx * Integer.BYTES, i);
            }
            return respectNulls;
        }
    }

    static class Decimal32LeadLagValueCurrentRow extends LeadLagWindowFunctionFactoryHelper.BaseLeadLagCurrentRow {

        private final int type;
        private int value;

        public Decimal32LeadLagValueCurrentRow(VirtualRecord partitionByRecord, Function arg, String name, boolean ignoreNulls, int type) {
            super(partitionByRecord, arg, name, ignoreNulls);
            this.type = type;
        }

        @Override
        public void computeNext(Record record) {
            value = arg.getDecimal32(record);
        }

        @Override
        public int getDecimal32(Record rec) {
            return value;
        }

        @Override
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putInt(spi.getAddress(recordOffset, columnIndex), value);
        }
    }

    public static class Decimal64LagFunction extends LeadLagWindowFunctionFactoryHelper.BaseLagFunction {

        private final int type;
        private long lagValue;

        public Decimal64LagFunction(Function arg, Function defaultValueFunc, long offset, MemoryARW memory, boolean ignoreNulls, int type) {
            super(arg, defaultValueFunc, offset, memory, ignoreNulls);
            this.type = type;
        }

        @Override
        public boolean computeNext0(Record record) {
            if (count < offset) {
                lagValue = defaultValue == null ? Decimals.DECIMAL64_NULL : defaultValue.getDecimal64(record);
            } else {
                lagValue = buffer.getLong((long) loIdx * Long.BYTES);
            }
            long l = arg.getDecimal64(record);
            boolean respectNulls = !ignoreNulls || l != Decimals.DECIMAL64_NULL;
            if (respectNulls) {
                buffer.putLong((long) loIdx * Long.BYTES, l);
            }
            return respectNulls;
        }

        @Override
        public long getDecimal64(Record rec) {
            return lagValue;
        }

        @Override
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putLong(spi.getAddress(recordOffset, columnIndex), lagValue);
        }
    }

    static class Decimal64LagOverPartitionFunction extends LeadLagWindowFunctionFactoryHelper.BaseLagOverPartitionFunction {

        private final int type;
        private long lagValue;

        public Decimal64LagOverPartitionFunction(Map map,
                                                 VirtualRecord partitionByRecord,
                                                 RecordSink partitionBySink,
                                                 MemoryARW memory,
                                                 Function arg,
                                                 boolean ignoreNulls,
                                                 Function defaultValue,
                                                 long offset,
                                                 int type) {
            super(map, partitionByRecord, partitionBySink, memory, arg, ignoreNulls, defaultValue, offset);
            this.type = type;
        }

        @Override
        public long getDecimal64(Record rec) {
            return lagValue;
        }

        @Override
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putLong(spi.getAddress(recordOffset, columnIndex), lagValue);
        }

        @Override
        protected boolean computeNext0(long count, long offset, long startOffset, long firstIdx, Record record) {
            long l = arg.getDecimal64(record);
            if (count < offset) {
                lagValue = defaultValue == null ? Decimals.DECIMAL64_NULL : defaultValue.getDecimal64(record);
            } else {
                lagValue = memory.getLong(startOffset + firstIdx * Long.BYTES);
            }
            boolean respectNulls = !ignoreNulls || Decimals.DECIMAL64_NULL != l;
            if (respectNulls) {
                memory.putLong(startOffset + firstIdx * Long.BYTES, l);
            }
            return respectNulls;
        }
    }

    static class Decimal64LeadLagValueCurrentRow extends LeadLagWindowFunctionFactoryHelper.BaseLeadLagCurrentRow {

        private final int type;
        private long value;

        public Decimal64LeadLagValueCurrentRow(VirtualRecord partitionByRecord, Function arg, String name, boolean ignoreNulls, int type) {
            super(partitionByRecord, arg, name, ignoreNulls);
            this.type = type;
        }

        @Override
        public void computeNext(Record record) {
            value = arg.getDecimal64(record);
        }

        @Override
        public long getDecimal64(Record rec) {
            return value;
        }

        @Override
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putLong(spi.getAddress(recordOffset, columnIndex), value);
        }
    }

    public static class Decimal8LagFunction extends LeadLagWindowFunctionFactoryHelper.BaseLagFunction {

        private final int type;
        private byte lagValue;

        public Decimal8LagFunction(Function arg, Function defaultValueFunc, long offset, MemoryARW memory, boolean ignoreNulls, int type) {
            super(arg, defaultValueFunc, offset, memory, ignoreNulls);
            this.type = type;
        }

        @Override
        public boolean computeNext0(Record record) {
            if (count < offset) {
                lagValue = defaultValue == null ? Decimals.DECIMAL8_NULL : defaultValue.getDecimal8(record);
            } else {
                lagValue = buffer.getByte((long) loIdx * Byte.BYTES);
            }
            byte b = arg.getDecimal8(record);
            boolean respectNulls = !ignoreNulls || b != Decimals.DECIMAL8_NULL;
            if (respectNulls) {
                buffer.putByte((long) loIdx * Byte.BYTES, b);
            }
            return respectNulls;
        }

        @Override
        public byte getDecimal8(Record rec) {
            return lagValue;
        }

        @Override
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putByte(spi.getAddress(recordOffset, columnIndex), lagValue);
        }
    }

    static class Decimal8LagOverPartitionFunction extends LeadLagWindowFunctionFactoryHelper.BaseLagOverPartitionFunction {

        private final int type;
        private byte lagValue;

        public Decimal8LagOverPartitionFunction(Map map,
                                                VirtualRecord partitionByRecord,
                                                RecordSink partitionBySink,
                                                MemoryARW memory,
                                                Function arg,
                                                boolean ignoreNulls,
                                                Function defaultValue,
                                                long offset,
                                                int type) {
            super(map, partitionByRecord, partitionBySink, memory, arg, ignoreNulls, defaultValue, offset);
            this.type = type;
        }

        @Override
        public byte getDecimal8(Record rec) {
            return lagValue;
        }

        @Override
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putByte(spi.getAddress(recordOffset, columnIndex), lagValue);
        }

        @Override
        protected boolean computeNext0(long count, long offset, long startOffset, long firstIdx, Record record) {
            byte b = arg.getDecimal8(record);
            if (count < offset) {
                lagValue = defaultValue == null ? Decimals.DECIMAL8_NULL : defaultValue.getDecimal8(record);
            } else {
                lagValue = memory.getByte(startOffset + firstIdx * Byte.BYTES);
            }
            boolean respectNulls = !ignoreNulls || Decimals.DECIMAL8_NULL != b;
            if (respectNulls) {
                memory.putByte(startOffset + firstIdx * Byte.BYTES, b);
            }
            return respectNulls;
        }
    }

    static class Decimal8LeadLagValueCurrentRow extends LeadLagWindowFunctionFactoryHelper.BaseLeadLagCurrentRow {

        private final int type;
        private byte value;

        public Decimal8LeadLagValueCurrentRow(VirtualRecord partitionByRecord, Function arg, String name, boolean ignoreNulls, int type) {
            super(partitionByRecord, arg, name, ignoreNulls);
            this.type = type;
        }

        @Override
        public void computeNext(Record record) {
            value = arg.getDecimal8(record);
        }

        @Override
        public byte getDecimal8(Record rec) {
            return value;
        }

        @Override
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putByte(spi.getAddress(recordOffset, columnIndex), value);
        }
    }
}
