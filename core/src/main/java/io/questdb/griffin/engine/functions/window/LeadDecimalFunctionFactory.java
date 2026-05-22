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
import io.questdb.cairo.Reopenable;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
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

public class LeadDecimalFunctionFactory extends AbstractWindowFunctionFactory {

    private static final String SIGNATURE = LeadLagWindowFunctionFactoryHelper.LEAD_NAME + "(ΞV)";

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
        switch (tag) {
            case ColumnType.DECIMAL8:
                return LeadLagWindowFunctionFactoryHelper.newInstance(
                        position, args, argPositions, configuration, sqlExecutionContext,
                        (defaultValue) -> {
                            if (!ColumnType.isSameOrBuiltInWideningCast(defaultValue.getType(), argType)) {
                                throw SqlException.$(argPositions.getQuick(2), "default value must be can cast to decimal");
                            }
                        },
                        (arg, defaultValueFunc, offset, memory, ignoreNulls) -> new Decimal8LeadFunction(arg, defaultValueFunc, offset, memory, ignoreNulls, argType),
                        (partitionByRecord, arg, name, ignoreNulls) -> new LagDecimalFunctionFactory.Decimal8LeadLagValueCurrentRow(partitionByRecord, arg, name, ignoreNulls, argType),
                        (map, partitionByRecord, partitionBySink, memory, arg, ignoreNulls, defaultValue, offset) -> new Decimal8LeadOverPartitionFunction(map, partitionByRecord, partitionBySink, memory, arg, ignoreNulls, defaultValue, offset, argType)
                );
            case ColumnType.DECIMAL16:
                return LeadLagWindowFunctionFactoryHelper.newInstance(
                        position, args, argPositions, configuration, sqlExecutionContext,
                        (defaultValue) -> {
                            if (!ColumnType.isSameOrBuiltInWideningCast(defaultValue.getType(), argType)) {
                                throw SqlException.$(argPositions.getQuick(2), "default value must be can cast to decimal");
                            }
                        },
                        (arg, defaultValueFunc, offset, memory, ignoreNulls) -> new Decimal16LeadFunction(arg, defaultValueFunc, offset, memory, ignoreNulls, argType),
                        (partitionByRecord, arg, name, ignoreNulls) -> new LagDecimalFunctionFactory.Decimal16LeadLagValueCurrentRow(partitionByRecord, arg, name, ignoreNulls, argType),
                        (map, partitionByRecord, partitionBySink, memory, arg, ignoreNulls, defaultValue, offset) -> new Decimal16LeadOverPartitionFunction(map, partitionByRecord, partitionBySink, memory, arg, ignoreNulls, defaultValue, offset, argType)
                );
            case ColumnType.DECIMAL32:
                return LeadLagWindowFunctionFactoryHelper.newInstance(
                        position, args, argPositions, configuration, sqlExecutionContext,
                        (defaultValue) -> {
                            if (!ColumnType.isSameOrBuiltInWideningCast(defaultValue.getType(), argType)) {
                                throw SqlException.$(argPositions.getQuick(2), "default value must be can cast to decimal");
                            }
                        },
                        (arg, defaultValueFunc, offset, memory, ignoreNulls) -> new Decimal32LeadFunction(arg, defaultValueFunc, offset, memory, ignoreNulls, argType),
                        (partitionByRecord, arg, name, ignoreNulls) -> new LagDecimalFunctionFactory.Decimal32LeadLagValueCurrentRow(partitionByRecord, arg, name, ignoreNulls, argType),
                        (map, partitionByRecord, partitionBySink, memory, arg, ignoreNulls, defaultValue, offset) -> new Decimal32LeadOverPartitionFunction(map, partitionByRecord, partitionBySink, memory, arg, ignoreNulls, defaultValue, offset, argType)
                );
            case ColumnType.DECIMAL64:
                return LeadLagWindowFunctionFactoryHelper.newInstance(
                        position, args, argPositions, configuration, sqlExecutionContext,
                        (defaultValue) -> {
                            if (!ColumnType.isSameOrBuiltInWideningCast(defaultValue.getType(), argType)) {
                                throw SqlException.$(argPositions.getQuick(2), "default value must be can cast to decimal");
                            }
                        },
                        (arg, defaultValueFunc, offset, memory, ignoreNulls) -> new Decimal64LeadFunction(arg, defaultValueFunc, offset, memory, ignoreNulls, argType),
                        (partitionByRecord, arg, name, ignoreNulls) -> new LagDecimalFunctionFactory.Decimal64LeadLagValueCurrentRow(partitionByRecord, arg, name, ignoreNulls, argType),
                        (map, partitionByRecord, partitionBySink, memory, arg, ignoreNulls, defaultValue, offset) -> new Decimal64LeadOverPartitionFunction(map, partitionByRecord, partitionBySink, memory, arg, ignoreNulls, defaultValue, offset, argType)
                );
            case ColumnType.DECIMAL128:
                return LeadLagWindowFunctionFactoryHelper.newInstance(
                        position, args, argPositions, configuration, sqlExecutionContext,
                        (defaultValue) -> {
                            if (!ColumnType.isSameOrBuiltInWideningCast(defaultValue.getType(), argType)) {
                                throw SqlException.$(argPositions.getQuick(2), "default value must be can cast to decimal");
                            }
                        },
                        (arg, defaultValueFunc, offset, memory, ignoreNulls) -> new Decimal128LeadFunction(arg, defaultValueFunc, offset, memory, ignoreNulls, argType),
                        (partitionByRecord, arg, name, ignoreNulls) -> new LagDecimalFunctionFactory.Decimal128LeadLagValueCurrentRow(partitionByRecord, arg, name, ignoreNulls, argType),
                        (map, partitionByRecord, partitionBySink, memory, arg, ignoreNulls, defaultValue, offset) -> new Decimal128LeadOverPartitionFunction(map, partitionByRecord, partitionBySink, memory, arg, ignoreNulls, defaultValue, offset, argType)
                );
            case ColumnType.DECIMAL256:
                return LeadLagWindowFunctionFactoryHelper.newInstance(
                        position, args, argPositions, configuration, sqlExecutionContext,
                        (defaultValue) -> {
                            if (!ColumnType.isSameOrBuiltInWideningCast(defaultValue.getType(), argType)) {
                                throw SqlException.$(argPositions.getQuick(2), "default value must be can cast to decimal");
                            }
                        },
                        (arg, defaultValueFunc, offset, memory, ignoreNulls) -> new Decimal256LeadFunction(arg, defaultValueFunc, offset, memory, ignoreNulls, argType),
                        (partitionByRecord, arg, name, ignoreNulls) -> new LagDecimalFunctionFactory.Decimal256LeadLagValueCurrentRow(partitionByRecord, arg, name, ignoreNulls, argType),
                        (map, partitionByRecord, partitionBySink, memory, arg, ignoreNulls, defaultValue, offset) -> new Decimal256LeadOverPartitionFunction(map, partitionByRecord, partitionBySink, memory, arg, ignoreNulls, defaultValue, offset, argType)
                );
            default:
                throw SqlException.$(position, "lead is not yet implemented for ").put(ColumnType.nameOf(tag));
        }
    }

    static class Decimal128LeadFunction extends LeadLagWindowFunctionFactoryHelper.BaseLeadFunction implements Reopenable, WindowDecimal128Function {

        private final Decimal128 scratch = new Decimal128();
        private final int type;

        public Decimal128LeadFunction(Function arg, Function defaultValueFunc, long offset, MemoryARW memory, boolean ignoreNulls, int type) {
            super(arg, defaultValueFunc, offset, memory, ignoreNulls);
            this.type = type;
        }

        @Override
        public int getType() {
            return type;
        }

        @Override
        protected boolean doPass1(Record record, long recordOffset, WindowSPI spi) {
            Decimal128 leadValue = new Decimal128();
            if (count < offset) {
                if (defaultValue == null) {
                    leadValue.ofRawNull();
                } else {
                    defaultValue.getDecimal128(record, leadValue);
                }
            } else {
                buffer.getDecimal128((long) loIdx * 16L, leadValue);
            }
            arg.getDecimal128(record, scratch);
            boolean respectNull = !ignoreNulls || !scratch.isNull();
            if (respectNull) {
                buffer.putDecimal128((long) loIdx * 16L, scratch.getHigh(), scratch.getLow());
            }
            long addr = spi.getAddress(recordOffset, columnIndex);
            Unsafe.putLong(addr, leadValue.getHigh());
            Unsafe.putLong(addr + Long.BYTES, leadValue.getLow());
            return respectNull;
        }
    }

    static class Decimal128LeadOverPartitionFunction extends BasePartitionedWindowFunction implements WindowDecimal128Function {

        private final Function defaultValue;
        private final boolean ignoreNulls;
        private final MemoryARW memory;
        private final long offset;
        private final Decimal128 scratch = new Decimal128();
        private final int type;

        public Decimal128LeadOverPartitionFunction(Map map,
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
        }

        @Override
        public void close() {
            super.close();
            Misc.free(memory);
            Misc.free(defaultValue);
        }

        @Override
        public String getName() {
            return LeadLagWindowFunctionFactoryHelper.LEAD_NAME;
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
        public boolean isIgnoreNulls() {
            return ignoreNulls;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mapValue = key.createValue();
            long startOffset;
            long firstIdx;
            long count = 0;

            if (mapValue.isNew()) {
                startOffset = memory.appendAddressFor(offset * 16L) - memory.getPageAddress(0);
                firstIdx = 0;
                Decimal128 nullScratch = new Decimal128();
                nullScratch.ofRawNull();
                for (long i = 0; i < offset; i++) {
                    memory.putDecimal128(startOffset + i * 16L, nullScratch.getHigh(), nullScratch.getLow());
                }
            } else {
                startOffset = mapValue.getLong(0);
                firstIdx = mapValue.getLong(1);
                count = mapValue.getLong(2);
            }

            arg.getDecimal128(record, scratch);
            Decimal128 leadValue = new Decimal128();
            if (count < offset) {
                if (defaultValue == null) {
                    leadValue.ofRawNull();
                } else {
                    defaultValue.getDecimal128(record, leadValue);
                }
            } else {
                memory.getDecimal128(startOffset + firstIdx * 16L, leadValue);
            }
            boolean respectNulls = !ignoreNulls || !scratch.isNull();
            if (respectNulls) {
                memory.putDecimal128(startOffset + firstIdx * 16L, scratch.getHigh(), scratch.getLow());
                firstIdx++;
                count++;
            }
            long addr = spi.getAddress(recordOffset, columnIndex);
            Unsafe.putLong(addr, leadValue.getHigh());
            Unsafe.putLong(addr + Long.BYTES, leadValue.getLow());

            mapValue.putLong(0, startOffset);
            mapValue.putLong(1, firstIdx % offset);
            mapValue.putLong(2, count);
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

    static class Decimal256LeadFunction extends LeadLagWindowFunctionFactoryHelper.BaseLeadFunction implements Reopenable, WindowDecimal256Function {

        private final Decimal256 scratch = new Decimal256();
        private final int type;

        public Decimal256LeadFunction(Function arg, Function defaultValueFunc, long offset, MemoryARW memory, boolean ignoreNulls, int type) {
            super(arg, defaultValueFunc, offset, memory, ignoreNulls);
            this.type = type;
        }

        @Override
        public int getType() {
            return type;
        }

        @Override
        protected boolean doPass1(Record record, long recordOffset, WindowSPI spi) {
            Decimal256 leadValue = new Decimal256();
            if (count < offset) {
                if (defaultValue == null) {
                    leadValue.ofRawNull();
                } else {
                    defaultValue.getDecimal256(record, leadValue);
                }
            } else {
                buffer.getDecimal256((long) loIdx * 32L, leadValue);
            }
            arg.getDecimal256(record, scratch);
            boolean respectNull = !ignoreNulls || !scratch.isNull();
            if (respectNull) {
                buffer.putDecimal256((long) loIdx * 32L, scratch.getHh(), scratch.getHl(), scratch.getLh(), scratch.getLl());
            }
            long addr = spi.getAddress(recordOffset, columnIndex);
            Unsafe.putLong(addr, leadValue.getHh());
            Unsafe.putLong(addr + Long.BYTES, leadValue.getHl());
            Unsafe.putLong(addr + 2 * Long.BYTES, leadValue.getLh());
            Unsafe.putLong(addr + 3 * Long.BYTES, leadValue.getLl());
            return respectNull;
        }
    }

    static class Decimal256LeadOverPartitionFunction extends BasePartitionedWindowFunction implements WindowDecimal256Function {

        private final Function defaultValue;
        private final boolean ignoreNulls;
        private final MemoryARW memory;
        private final long offset;
        private final Decimal256 scratch = new Decimal256();
        private final int type;

        public Decimal256LeadOverPartitionFunction(Map map,
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
        }

        @Override
        public void close() {
            super.close();
            Misc.free(memory);
            Misc.free(defaultValue);
        }

        @Override
        public String getName() {
            return LeadLagWindowFunctionFactoryHelper.LEAD_NAME;
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
        public boolean isIgnoreNulls() {
            return ignoreNulls;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mapValue = key.createValue();
            long startOffset;
            long firstIdx;
            long count = 0;

            if (mapValue.isNew()) {
                startOffset = memory.appendAddressFor(offset * 32L) - memory.getPageAddress(0);
                firstIdx = 0;
                Decimal256 nullScratch = new Decimal256();
                nullScratch.ofRawNull();
                for (long i = 0; i < offset; i++) {
                    memory.putDecimal256(startOffset + i * 32L, nullScratch.getHh(), nullScratch.getHl(), nullScratch.getLh(), nullScratch.getLl());
                }
            } else {
                startOffset = mapValue.getLong(0);
                firstIdx = mapValue.getLong(1);
                count = mapValue.getLong(2);
            }

            arg.getDecimal256(record, scratch);
            Decimal256 leadValue = new Decimal256();
            if (count < offset) {
                if (defaultValue == null) {
                    leadValue.ofRawNull();
                } else {
                    defaultValue.getDecimal256(record, leadValue);
                }
            } else {
                memory.getDecimal256(startOffset + firstIdx * 32L, leadValue);
            }
            boolean respectNulls = !ignoreNulls || !scratch.isNull();
            if (respectNulls) {
                memory.putDecimal256(startOffset + firstIdx * 32L, scratch.getHh(), scratch.getHl(), scratch.getLh(), scratch.getLl());
                firstIdx++;
                count++;
            }
            long addr = spi.getAddress(recordOffset, columnIndex);
            Unsafe.putLong(addr, leadValue.getHh());
            Unsafe.putLong(addr + Long.BYTES, leadValue.getHl());
            Unsafe.putLong(addr + 2 * Long.BYTES, leadValue.getLh());
            Unsafe.putLong(addr + 3 * Long.BYTES, leadValue.getLl());

            mapValue.putLong(0, startOffset);
            mapValue.putLong(1, firstIdx % offset);
            mapValue.putLong(2, count);
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

    static class Decimal64LeadFunction extends LeadLagWindowFunctionFactoryHelper.BaseLeadFunction implements Reopenable, WindowDecimal64Function {

        private final int type;

        public Decimal64LeadFunction(Function arg, Function defaultValueFunc, long offset, MemoryARW memory, boolean ignoreNulls, int type) {
            super(arg, defaultValueFunc, offset, memory, ignoreNulls);
            this.type = type;
        }

        @Override
        public int getType() {
            return type;
        }

        @Override
        protected boolean doPass1(Record record, long recordOffset, WindowSPI spi) {
            long leadValue;
            if (count < offset) {
                leadValue = defaultValue == null ? Decimals.DECIMAL64_NULL : defaultValue.getDecimal64(record);
            } else {
                leadValue = buffer.getLong((long) loIdx * Long.BYTES);
            }
            long l = arg.getDecimal64(record);
            boolean respectNull = !ignoreNulls || l != Decimals.DECIMAL64_NULL;
            if (respectNull) {
                buffer.putLong((long) loIdx * Long.BYTES, l);
            }
            Unsafe.putLong(spi.getAddress(recordOffset, columnIndex), leadValue);
            return respectNull;
        }
    }

    static class Decimal64LeadOverPartitionFunction extends LeadLagWindowFunctionFactoryHelper.BaseLeadOverPartitionFunction implements WindowDecimal64Function {

        private final int type;

        public Decimal64LeadOverPartitionFunction(Map map,
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
        public int getType() {
            return type;
        }

        @Override
        protected boolean doPass1(long count,
                                  long offset,
                                  long startOffset,
                                  long firstIdx,
                                  Record record,
                                  long recordOffset,
                                  WindowSPI spi) {
            long l = arg.getDecimal64(record);
            long leadValue;
            if (count < offset) {
                leadValue = defaultValue == null ? Decimals.DECIMAL64_NULL : defaultValue.getDecimal64(record);
            } else {
                leadValue = memory.getLong(startOffset + firstIdx * Long.BYTES);
            }
            boolean respectNulls = !ignoreNulls || l != Decimals.DECIMAL64_NULL;
            if (respectNulls) {
                memory.putLong(startOffset + firstIdx * Long.BYTES, l);
            }
            Unsafe.putLong(spi.getAddress(recordOffset, columnIndex), leadValue);
            return respectNulls;
        }
    }

    static class Decimal8LeadFunction extends LeadLagWindowFunctionFactoryHelper.BaseLeadFunction implements Reopenable, WindowDecimal8Function {

        private final int type;

        public Decimal8LeadFunction(Function arg, Function defaultValueFunc, long offset, MemoryARW memory, boolean ignoreNulls, int type) {
            super(arg, defaultValueFunc, offset, memory, ignoreNulls);
            this.type = type;
        }

        @Override
        public int getType() {
            return type;
        }

        @Override
        protected boolean doPass1(Record record, long recordOffset, WindowSPI spi) {
            byte leadValue;
            if (count < offset) {
                leadValue = defaultValue == null ? Decimals.DECIMAL8_NULL : defaultValue.getDecimal8(record);
            } else {
                leadValue = buffer.getByte((long) loIdx * Long.BYTES);
            }
            byte b = arg.getDecimal8(record);
            boolean respectNull = !ignoreNulls || b != Decimals.DECIMAL8_NULL;
            if (respectNull) {
                buffer.putByte((long) loIdx * Long.BYTES, b);
            }
            Unsafe.putByte(spi.getAddress(recordOffset, columnIndex), leadValue);
            return respectNull;
        }
    }

    static class Decimal8LeadOverPartitionFunction extends LeadLagWindowFunctionFactoryHelper.BaseLeadOverPartitionFunction implements WindowDecimal8Function {

        private final int type;

        public Decimal8LeadOverPartitionFunction(Map map,
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
        public int getType() {
            return type;
        }

        @Override
        protected boolean doPass1(long count,
                                  long offset,
                                  long startOffset,
                                  long firstIdx,
                                  Record record,
                                  long recordOffset,
                                  WindowSPI spi) {
            byte b = arg.getDecimal8(record);
            byte leadValue;
            if (count < offset) {
                leadValue = defaultValue == null ? Decimals.DECIMAL8_NULL : defaultValue.getDecimal8(record);
            } else {
                leadValue = memory.getByte(startOffset + firstIdx * Long.BYTES);
            }
            boolean respectNulls = !ignoreNulls || b != Decimals.DECIMAL8_NULL;
            if (respectNulls) {
                memory.putByte(startOffset + firstIdx * Long.BYTES, b);
            }
            Unsafe.putByte(spi.getAddress(recordOffset, columnIndex), leadValue);
            return respectNulls;
        }
    }

    static class Decimal16LeadFunction extends LeadLagWindowFunctionFactoryHelper.BaseLeadFunction implements Reopenable, WindowDecimal16Function {

        private final int type;

        public Decimal16LeadFunction(Function arg, Function defaultValueFunc, long offset, MemoryARW memory, boolean ignoreNulls, int type) {
            super(arg, defaultValueFunc, offset, memory, ignoreNulls);
            this.type = type;
        }

        @Override
        public int getType() {
            return type;
        }

        @Override
        protected boolean doPass1(Record record, long recordOffset, WindowSPI spi) {
            short leadValue;
            if (count < offset) {
                leadValue = defaultValue == null ? Decimals.DECIMAL16_NULL : defaultValue.getDecimal16(record);
            } else {
                leadValue = buffer.getShort((long) loIdx * Long.BYTES);
            }
            short s = arg.getDecimal16(record);
            boolean respectNull = !ignoreNulls || s != Decimals.DECIMAL16_NULL;
            if (respectNull) {
                buffer.putShort((long) loIdx * Long.BYTES, s);
            }
            Unsafe.putShort(spi.getAddress(recordOffset, columnIndex), leadValue);
            return respectNull;
        }
    }

    static class Decimal16LeadOverPartitionFunction extends LeadLagWindowFunctionFactoryHelper.BaseLeadOverPartitionFunction implements WindowDecimal16Function {

        private final int type;

        public Decimal16LeadOverPartitionFunction(Map map,
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
        public int getType() {
            return type;
        }

        @Override
        protected boolean doPass1(long count,
                                  long offset,
                                  long startOffset,
                                  long firstIdx,
                                  Record record,
                                  long recordOffset,
                                  WindowSPI spi) {
            short s = arg.getDecimal16(record);
            short leadValue;
            if (count < offset) {
                leadValue = defaultValue == null ? Decimals.DECIMAL16_NULL : defaultValue.getDecimal16(record);
            } else {
                leadValue = memory.getShort(startOffset + firstIdx * Long.BYTES);
            }
            boolean respectNulls = !ignoreNulls || s != Decimals.DECIMAL16_NULL;
            if (respectNulls) {
                memory.putShort(startOffset + firstIdx * Long.BYTES, s);
            }
            Unsafe.putShort(spi.getAddress(recordOffset, columnIndex), leadValue);
            return respectNulls;
        }
    }

    static class Decimal32LeadFunction extends LeadLagWindowFunctionFactoryHelper.BaseLeadFunction implements Reopenable, WindowDecimal32Function {

        private final int type;

        public Decimal32LeadFunction(Function arg, Function defaultValueFunc, long offset, MemoryARW memory, boolean ignoreNulls, int type) {
            super(arg, defaultValueFunc, offset, memory, ignoreNulls);
            this.type = type;
        }

        @Override
        public int getType() {
            return type;
        }

        @Override
        protected boolean doPass1(Record record, long recordOffset, WindowSPI spi) {
            int leadValue;
            if (count < offset) {
                leadValue = defaultValue == null ? Decimals.DECIMAL32_NULL : defaultValue.getDecimal32(record);
            } else {
                leadValue = buffer.getInt((long) loIdx * Long.BYTES);
            }
            int i = arg.getDecimal32(record);
            boolean respectNull = !ignoreNulls || i != Decimals.DECIMAL32_NULL;
            if (respectNull) {
                buffer.putInt((long) loIdx * Long.BYTES, i);
            }
            Unsafe.putInt(spi.getAddress(recordOffset, columnIndex), leadValue);
            return respectNull;
        }
    }

    static class Decimal32LeadOverPartitionFunction extends LeadLagWindowFunctionFactoryHelper.BaseLeadOverPartitionFunction implements WindowDecimal32Function {

        private final int type;

        public Decimal32LeadOverPartitionFunction(Map map,
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
        public int getType() {
            return type;
        }

        @Override
        protected boolean doPass1(long count,
                                  long offset,
                                  long startOffset,
                                  long firstIdx,
                                  Record record,
                                  long recordOffset,
                                  WindowSPI spi) {
            int i = arg.getDecimal32(record);
            int leadValue;
            if (count < offset) {
                leadValue = defaultValue == null ? Decimals.DECIMAL32_NULL : defaultValue.getDecimal32(record);
            } else {
                leadValue = memory.getInt(startOffset + firstIdx * Long.BYTES);
            }
            boolean respectNulls = !ignoreNulls || i != Decimals.DECIMAL32_NULL;
            if (respectNulls) {
                memory.putInt(startOffset + firstIdx * Long.BYTES, i);
            }
            Unsafe.putInt(spi.getAddress(recordOffset, columnIndex), leadValue);
            return respectNulls;
        }
    }
}
