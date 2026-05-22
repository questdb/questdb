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

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypes;
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
import io.questdb.griffin.engine.window.WindowContext;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.griffin.model.WindowExpression;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimals;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;

public class NthValueDecimalWindowFunctionFactory extends AbstractWindowFunctionFactory {

    public static final String NAME = "nth_value";
    private static final ArrayColumnTypes NTH_VALUE_DECIMAL64_TYPES;
    private static final String SIGNATURE = NAME + "(ΞL)";

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
        WindowContext windowContext = sqlExecutionContext.getWindowContext();
        windowContext.validate(position, supportNullsDesc());

        final int argType = args.get(0).getType();
        final int tag = ColumnType.tagOf(argType);

        Function nFunc = args.get(1);
        if (!nFunc.isConstant()) {
            throw SqlException.$(argPositions.getQuick(1), "n must be a constant");
        }
        long nLong = nFunc.getLong(null);
        if (nFunc.isNullConstant() || nLong == Numbers.LONG_NULL || nLong == Numbers.INT_NULL) {
            throw SqlException.$(argPositions.getQuick(1), "n cannot be NULL");
        }
        if (nLong <= 0 || nLong > Integer.MAX_VALUE) {
            throw SqlException.$(argPositions.getQuick(1), "n must be a positive integer");
        }
        int n = (int) nLong;

        long rowsLo = windowContext.getRowsLo();
        long rowsHi = windowContext.getRowsHi();
        if (rowsHi < rowsLo) {
            boolean isRange = windowContext.getFramingMode() == WindowExpression.FRAMING_RANGE;
            VirtualRecord partitionByRecord = windowContext.getPartitionByRecord();
            switch (tag) {
                case ColumnType.DECIMAL8:
                    return new Decimal8NullFunction(args.get(0), NAME, rowsLo, rowsHi, isRange, partitionByRecord, argType);
                case ColumnType.DECIMAL16:
                    return new Decimal16NullFunction(args.get(0), NAME, rowsLo, rowsHi, isRange, partitionByRecord, argType);
                case ColumnType.DECIMAL32:
                    return new Decimal32NullFunction(args.get(0), NAME, rowsLo, rowsHi, isRange, partitionByRecord, argType);
                case ColumnType.DECIMAL64:
                    return new Decimal64NullFunction(args.get(0), NAME, rowsLo, rowsHi, isRange, partitionByRecord, argType);
                case ColumnType.DECIMAL128:
                    return new Decimal128NullFunction(args.get(0), NAME, rowsLo, rowsHi, isRange, partitionByRecord, argType);
                default:
                    return new Decimal256NullFunction(args.get(0), NAME, rowsLo, rowsHi, isRange, partitionByRecord, argType);
            }
        }

        if (tag != ColumnType.DECIMAL64 && tag != ColumnType.DECIMAL8 && tag != ColumnType.DECIMAL16 && tag != ColumnType.DECIMAL32 && tag != ColumnType.DECIMAL128 && tag != ColumnType.DECIMAL256) {
            throw SqlException.$(position, "nth_value is not yet implemented for ").put(ColumnType.nameOf(tag));
        }
        if (tag == ColumnType.DECIMAL8) {
            return newInstanceDecimal8(position, args, configuration, windowContext, n, argType);
        }
        if (tag == ColumnType.DECIMAL16) {
            return newInstanceDecimal16(position, args, configuration, windowContext, n, argType);
        }
        if (tag == ColumnType.DECIMAL32) {
            return newInstanceDecimal32(position, args, configuration, windowContext, n, argType);
        }
        if (tag == ColumnType.DECIMAL128) {
            return newInstanceDecimal128(position, args, configuration, windowContext, n, argType);
        }
        if (tag == ColumnType.DECIMAL256) {
            return newInstanceDecimal256(position, args, configuration, windowContext, n, argType);
        }

        if (windowContext.getFramingMode() == WindowExpression.FRAMING_ROWS) {
            if (rowsLo != Long.MIN_VALUE && Math.abs(rowsLo) > Integer.MAX_VALUE) {
                throw SqlException.$(windowContext.getRowsLoKindPos(), "frame start exceeds maximum supported size");
            }
            if (rowsHi != Long.MAX_VALUE && Math.abs(rowsHi) > Integer.MAX_VALUE) {
                throw SqlException.$(windowContext.getRowsHiKindPos(), "frame end exceeds maximum supported size");
            }
        }

        int framingMode = windowContext.getFramingMode();
        RecordSink partitionBySink = windowContext.getPartitionBySink();
        ColumnTypes partitionByKeyTypes = windowContext.getPartitionByKeyTypes();
        VirtualRecord partitionByRecord = windowContext.getPartitionByRecord();

        if (partitionByRecord != null) {
            if (framingMode == WindowExpression.FRAMING_RANGE) {
                if (windowContext.isDefaultFrame() && (!windowContext.isOrdered() || windowContext.getRowsHi() == Long.MAX_VALUE)) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, NTH_VALUE_DECIMAL64_TYPES);
                    try {
                        return new Decimal64NthValueOverPartitionFunction(map, partitionByRecord, partitionBySink, args.get(0), n, argType);
                    } catch (Throwable t) {
                        Misc.free(map);
                        throw t;
                    }
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, NTH_VALUE_DECIMAL64_TYPES);
                    try {
                        return new Decimal64NthValueOverUnboundedPartitionFrameFunction(map, partitionByRecord, partitionBySink, args.get(0), n, true, argType);
                    } catch (Throwable t) {
                        Misc.free(map);
                        throw t;
                    }
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    ArrayColumnTypes columnTypes = new ArrayColumnTypes();
                    columnTypes.add(ColumnType.LONG);
                    columnTypes.add(ColumnType.LONG);
                    columnTypes.add(ColumnType.LONG);
                    columnTypes.add(ColumnType.LONG);
                    columnTypes.add(ColumnType.LONG);
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, columnTypes);
                    final int initialBufferSize = configuration.getSqlWindowInitialRangeBufferSize();
                    MemoryARW mem;
                    try {
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                    } catch (Throwable t) {
                        Misc.free(map);
                        throw t;
                    }
                    try {
                        return new Decimal64NthValueOverPartitionRangeFrameFunction(map, partitionByRecord, partitionBySink,
                                rowsLo, rowsHi, args.get(0), mem, initialBufferSize, timestampIndex, n, argType);
                    } catch (Throwable t) {
                        Misc.free(map);
                        Misc.free(mem);
                        throw t;
                    }
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, NTH_VALUE_DECIMAL64_TYPES);
                    try {
                        return new Decimal64NthValueOverUnboundedPartitionFrameFunction(map, partitionByRecord, partitionBySink, args.get(0), n, false, argType);
                    } catch (Throwable t) {
                        Misc.free(map);
                        throw t;
                    }
                } else if (rowsLo == 0 && rowsHi == 0) {
                    return new Decimal64NthValueOverCurrentRowFunction(args.get(0), n, argType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, NTH_VALUE_DECIMAL64_TYPES);
                    try {
                        return new Decimal64NthValueOverPartitionFunction(map, partitionByRecord, partitionBySink, args.get(0), n, argType);
                    } catch (Throwable t) {
                        Misc.free(map);
                        throw t;
                    }
                } else if (rowsLo == Long.MIN_VALUE) {
                    ArrayColumnTypes columnTypes = new ArrayColumnTypes();
                    columnTypes.add(ColumnType.LONG);
                    columnTypes.add(ColumnType.LONG);
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, columnTypes);
                    try {
                        return new Decimal64NthValueOverPartitionRowsFrameUnboundedFunction(map, partitionByRecord, partitionBySink, rowsHi, args.get(0), n, argType);
                    } catch (Throwable t) {
                        Misc.free(map);
                        throw t;
                    }
                } else {
                    ArrayColumnTypes columnTypes = new ArrayColumnTypes();
                    columnTypes.add(ColumnType.LONG);
                    columnTypes.add(ColumnType.LONG);
                    columnTypes.add(ColumnType.LONG);
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, columnTypes);
                    MemoryARW mem;
                    try {
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                    } catch (Throwable t) {
                        Misc.free(map);
                        throw t;
                    }
                    try {
                        return new Decimal64NthValueOverPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink,
                                rowsLo, rowsHi, args.get(0), mem, n, argType);
                    } catch (Throwable t) {
                        Misc.free(map);
                        Misc.free(mem);
                        throw t;
                    }
                }
            }
        } else {
            if (framingMode == WindowExpression.FRAMING_RANGE) {
                if (!windowContext.isOrdered() && windowContext.isDefaultFrame()) {
                    return new Decimal64NthValueOverWholeResultSetFunction(args.get(0), n, argType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    return new Decimal64NthValueOverWholeResultSetFunction(args.get(0), n, argType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    return new Decimal64NthValueOverUnboundedRowsFrameFunction(args.get(0), n, argType);
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    return new Decimal64NthValueOverRangeFrameFunction(rowsLo, rowsHi, args.get(0), configuration, timestampIndex, n, argType);
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    return new Decimal64NthValueOverWholeResultSetFunction(args.get(0), n, argType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    return new Decimal64NthValueOverUnboundedRowsFrameFunction(args.get(0), n, argType);
                } else if (rowsLo == 0 && rowsHi == 0) {
                    return new Decimal64NthValueOverCurrentRowFunction(args.get(0), n, argType);
                } else if (rowsLo == Long.MIN_VALUE) {
                    return new Decimal64NthValueOverRowsFrameUnboundedFunction(args.get(0), rowsHi, n, argType);
                } else {
                    MemoryARW mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                            configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                    try {
                        return new Decimal64NthValueOverRowsFrameFunction(args.get(0), rowsLo, rowsHi, mem, n, argType);
                    } catch (Throwable t) {
                        Misc.free(mem);
                        throw t;
                    }
                }
            }
        }

        throw SqlException.$(position, "function not implemented for given window parameters");
    }

    static class Decimal64NthValueOverCurrentRowFunction extends BaseWindowFunction implements WindowDecimal64Function {

        private final int n;
        private final int type;
        private long value;

        Decimal64NthValueOverCurrentRowFunction(Function arg, int n, int type) {
            super(arg);
            this.n = n;
            this.type = type;
        }

        @Override
        public void computeNext(Record record) {
            value = n == 1 ? arg.getDecimal64(record) : Decimals.DECIMAL64_NULL;
        }

        @Override
        public long getDecimal64(Record rec) {
            return value;
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
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putLong(spi.getAddress(recordOffset, columnIndex), value);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(',').val(n).val(')');
            sink.val(" over (rows between current row and current row)");
        }
    }

    static class Decimal64NthValueOverPartitionFunction extends BasePartitionedWindowFunction implements WindowDecimal64Function {

        private final int n;
        private final int type;

        public Decimal64NthValueOverPartitionFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, int n, int type) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.n = n;
            this.type = type;
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public int getPassCount() {
            return WindowFunction.TWO_PASS;
        }

        @Override
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.createValue();

            if (value.isNew()) {
                if (n == 1) {
                    value.putLong(0, arg.getDecimal64(record));
                } else {
                    value.putLong(0, Decimals.DECIMAL64_NULL);
                }
                value.putLong(1, 1);
            } else {
                long count = value.getLong(1) + 1;
                value.putLong(1, count);
                if (count == n) {
                    value.putLong(0, arg.getDecimal64(record));
                }
            }
        }

        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.findValue();
            assert value != null;
            Unsafe.putLong(spi.getAddress(recordOffset, columnIndex), value.getLong(0));
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(',').val(n).val(')');
            sink.val(" over (");
            sink.val("partition by ");
            sink.val(partitionByRecord.getFunctions());
            sink.val(')');
        }
    }

    public static class Decimal64NthValueOverPartitionRangeFrameFunction extends BasePartitionedWindowFunction implements WindowDecimal64Function {

        protected static final int RECORD_SIZE = 2 * Long.BYTES;
        protected final boolean frameIncludesCurrentValue;
        protected final boolean frameLoBounded;
        protected final LongList freeList = new LongList();
        protected final int initialBufferSize;
        protected final long maxDiff;
        protected final MemoryARW memory;
        protected final RingBufferDesc memoryDesc = new RingBufferDesc();
        protected final long minDiff;
        protected final int n;
        protected final int timestampIndex;
        protected final int type;
        protected long nthValue;

        public Decimal64NthValueOverPartitionRangeFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rangeLo,
                long rangeHi,
                Function arg,
                MemoryARW memory,
                int initialBufferSize,
                int timestampIdx,
                int n,
                int type
        ) {
            super(map, partitionByRecord, partitionBySink, arg);
            frameLoBounded = rangeLo != Long.MIN_VALUE;
            maxDiff = frameLoBounded ? Math.abs(rangeLo) : Math.abs(rangeHi);
            minDiff = Math.abs(rangeHi);
            this.memory = memory;
            this.initialBufferSize = initialBufferSize;
            this.timestampIndex = timestampIdx;
            this.n = n;
            this.frameIncludesCurrentValue = rangeHi == 0;
            this.type = type;
        }

        @Override
        public void close() {
            super.close();
            memory.close();
            freeList.clear();
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mapValue = key.createValue();

            long frameSize;
            long startOffset;
            long size;
            long capacity;
            long firstIdx;
            long timestamp = record.getTimestamp(timestampIndex);
            long d = arg.getDecimal64(record);

            if (mapValue.isNew()) {
                capacity = initialBufferSize;
                startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
                firstIdx = 0;
                memory.putLong(startOffset, timestamp);
                memory.putLong(startOffset + Long.BYTES, d);
                size = 1;
                if (frameIncludesCurrentValue) {
                    frameSize = 1;
                    nthValue = n == 1 ? d : Decimals.DECIMAL64_NULL;
                } else {
                    frameSize = 0;
                    nthValue = Decimals.DECIMAL64_NULL;
                }
            } else {
                frameSize = mapValue.getLong(0);
                startOffset = mapValue.getLong(1);
                size = mapValue.getLong(2);
                capacity = mapValue.getLong(3);
                firstIdx = mapValue.getLong(4);

                if (!frameLoBounded && frameSize >= n) {
                    long nthIdx = (firstIdx + n - 1) % capacity;
                    nthValue = memory.getLong(startOffset + nthIdx * RECORD_SIZE + Long.BYTES);
                    return;
                }

                long newFirstIdx = firstIdx;
                if (frameLoBounded) {
                    for (long i = 0, nn = size; i < nn; i++) {
                        long idx = (firstIdx + i) % capacity;
                        long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                        if (Math.abs(timestamp - ts) > maxDiff) {
                            if (frameSize > 0) {
                                frameSize--;
                            }
                            newFirstIdx = (idx + 1) % capacity;
                            size--;
                        } else {
                            break;
                        }
                    }
                }
                firstIdx = newFirstIdx;

                if (size == capacity) {
                    memoryDesc.reset(capacity, startOffset, size, firstIdx, freeList);
                    expandRingBuffer(memory, memoryDesc, RECORD_SIZE);
                    capacity = memoryDesc.capacity;
                    startOffset = memoryDesc.startOffset;
                    firstIdx = memoryDesc.firstIdx;
                }
                memory.putLong(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE, timestamp);
                memory.putLong(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE + Long.BYTES, d);
                size++;

                if (frameLoBounded) {
                    for (long i = frameSize; i < size; i++) {
                        long idx = (firstIdx + i) % capacity;
                        long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                        long diff = Math.abs(ts - timestamp);
                        if (diff <= maxDiff && diff >= minDiff) {
                            frameSize++;
                        } else {
                            break;
                        }
                    }
                } else {
                    for (long i = frameSize; i < size; i++) {
                        long idx = (firstIdx + i) % capacity;
                        long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                        if (Math.abs(timestamp - ts) >= minDiff) {
                            frameSize++;
                        } else {
                            break;
                        }
                    }
                }

                if (frameSize >= n) {
                    long nthIdx = (firstIdx + n - 1) % capacity;
                    nthValue = memory.getLong(startOffset + nthIdx * RECORD_SIZE + Long.BYTES);
                } else {
                    nthValue = Decimals.DECIMAL64_NULL;
                }
            }

            mapValue.putLong(0, frameSize);
            mapValue.putLong(1, startOffset);
            mapValue.putLong(2, size);
            mapValue.putLong(3, capacity);
            mapValue.putLong(4, firstIdx);
        }

        @Override
        public long getDecimal64(Record rec) {
            return nthValue;
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
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putLong(spi.getAddress(recordOffset, columnIndex), nthValue);
        }

        @Override
        public void reopen() {
            super.reopen();
            freeList.clear();
            nthValue = Decimals.DECIMAL64_NULL;
        }

        @Override
        public void reset() {
            super.reset();
            memory.close();
            freeList.clear();
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(',').val(n).val(')');
            sink.val(" over (");
            sink.val("partition by ");
            sink.val(partitionByRecord.getFunctions());
            sink.val(" range between ");
            if (frameLoBounded) {
                sink.val(maxDiff).val(" preceding");
            } else {
                sink.val("unbounded preceding");
            }
            sink.val(" and ");
            if (minDiff == 0) {
                sink.val("current row");
            } else {
                sink.val(minDiff).val(" preceding");
            }
            sink.val(')');
        }

        @Override
        public void toTop() {
            super.toTop();
            memory.truncate();
            freeList.clear();
        }
    }

    public static class Decimal64NthValueOverPartitionRowsFrameFunction extends BasePartitionedWindowFunction implements WindowDecimal64Function {

        protected final int bufferSize;
        protected final int excludeCount;
        protected final boolean frameIncludesCurrentValue;
        protected final int frameSize;
        protected final MemoryARW memory;
        protected final int n;
        protected final int type;
        protected long nthValue;

        public Decimal64NthValueOverPartitionRowsFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rowsLo,
                long rowsHi,
                Function arg,
                MemoryARW memory,
                int n,
                int type
        ) {
            super(map, partitionByRecord, partitionBySink, arg);
            assert rowsLo > Long.MIN_VALUE;
            frameSize = (int) (rowsHi - rowsLo + (rowsHi < 0 ? 1 : 0));
            bufferSize = (int) Math.abs(rowsLo);
            this.excludeCount = rowsHi < 0 ? (int) Math.abs(rowsHi) : 0;
            this.frameIncludesCurrentValue = rowsHi == 0;
            this.memory = memory;
            this.n = n;
            this.type = type;
        }

        @Override
        public void close() {
            super.close();
            memory.close();
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mapValue = key.createValue();

            long d = arg.getDecimal64(record);

            long loIdx;
            long startOffset;
            long count;

            if (mapValue.isNew()) {
                loIdx = 0;
                count = 0;
                startOffset = memory.appendAddressFor((long) bufferSize * Long.BYTES) - memory.getPageAddress(0);
                mapValue.putLong(1, startOffset);
                for (int i = 0; i < bufferSize; i++) {
                    memory.putLong(startOffset + (long) i * Long.BYTES, Decimals.DECIMAL64_NULL);
                }
            } else {
                loIdx = mapValue.getLong(0);
                startOffset = mapValue.getLong(1);
                count = mapValue.getLong(2);
            }

            long effectiveCount = Math.min(count, bufferSize);
            long currentFrameSize;
            if (frameIncludesCurrentValue) {
                currentFrameSize = Math.min(effectiveCount, frameSize);
            } else {
                currentFrameSize = Math.clamp(count + 1 - excludeCount, 0, frameSize);
            }

            if (currentFrameSize > 0 || (count == 0 && frameIncludesCurrentValue)) {
                long frameStartIdx = (loIdx + bufferSize - effectiveCount) % bufferSize;
                if (frameIncludesCurrentValue) {
                    if (n <= currentFrameSize) {
                        long nthIdx = (frameStartIdx + n - 1) % bufferSize;
                        nthValue = memory.getLong(startOffset + nthIdx * Long.BYTES);
                    } else if (n == currentFrameSize + 1) {
                        nthValue = d;
                    } else {
                        nthValue = Decimals.DECIMAL64_NULL;
                    }
                } else {
                    if (n <= currentFrameSize) {
                        long nthIdx = (frameStartIdx + n - 1) % bufferSize;
                        nthValue = memory.getLong(startOffset + nthIdx * Long.BYTES);
                    } else {
                        nthValue = Decimals.DECIMAL64_NULL;
                    }
                }
            } else {
                nthValue = Decimals.DECIMAL64_NULL;
            }

            count = Math.min(count + 1, bufferSize);
            mapValue.putLong(0, (loIdx + 1) % bufferSize);
            mapValue.putLong(2, count);
            memory.putLong(startOffset + loIdx * Long.BYTES, d);
        }

        @Override
        public long getDecimal64(Record rec) {
            return nthValue;
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
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putLong(spi.getAddress(recordOffset, columnIndex), nthValue);
        }

        @Override
        public void reopen() {
            super.reopen();
        }

        @Override
        public void reset() {
            super.reset();
            memory.close();
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(',').val(n).val(')');
            sink.val(" over (");
            sink.val("partition by ");
            sink.val(partitionByRecord.getFunctions());
            sink.val(" rows between ");
            sink.val(bufferSize);
            sink.val(" preceding and ");
            if (frameIncludesCurrentValue) {
                sink.val("current row");
            } else {
                sink.val(excludeCount).val(" preceding");
            }
            sink.val(')');
        }

        @Override
        public void toTop() {
            super.toTop();
            memory.truncate();
        }
    }

    public static class Decimal64NthValueOverPartitionRowsFrameUnboundedFunction extends BasePartitionedWindowFunction implements WindowDecimal64Function {

        protected final int bufferSize;
        protected final int n;
        protected final int type;
        protected long nthValue;

        public Decimal64NthValueOverPartitionRowsFrameUnboundedFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rowsHi,
                Function arg,
                int n,
                int type
        ) {
            super(map, partitionByRecord, partitionBySink, arg);
            assert rowsHi < 0 && rowsHi != Long.MIN_VALUE;
            this.bufferSize = (int) Math.abs(rowsHi);
            this.n = n;
            this.type = type;
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mapValue = key.createValue();

            long d = arg.getDecimal64(record);
            long count;
            if (mapValue.isNew()) {
                count = 0;
                mapValue.putLong(1, Decimals.DECIMAL64_NULL);
            } else {
                count = mapValue.getLong(0);
            }
            count++;
            if (count == n) {
                mapValue.putLong(1, d);
            }
            if (count >= (long) n + bufferSize) {
                nthValue = mapValue.getLong(1);
            } else {
                nthValue = Decimals.DECIMAL64_NULL;
            }
            mapValue.putLong(0, count);
        }

        @Override
        public long getDecimal64(Record rec) {
            return nthValue;
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
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putLong(spi.getAddress(recordOffset, columnIndex), nthValue);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(',').val(n).val(')');
            sink.val(" over (");
            sink.val("partition by ");
            sink.val(partitionByRecord.getFunctions());
            sink.val(" rows between unbounded preceding and ");
            sink.val(bufferSize).val(" preceding");
            sink.val(')');
        }
    }

    public static class Decimal64NthValueOverRangeFrameFunction extends BaseWindowFunction implements Reopenable, WindowDecimal64Function {
        protected static final int RECORD_SIZE = 2 * Long.BYTES;
        protected final boolean frameIncludesCurrentValue;
        protected final boolean frameLoBounded;
        protected final LongList freeList = new LongList();
        protected final long initialCapacity;
        protected final long maxDiff;
        protected final MemoryARW memory;
        protected final RingBufferDesc memoryDesc = new RingBufferDesc();
        protected final long minDiff;
        protected final int n;
        protected final int timestampIndex;
        protected final int type;
        protected long capacity;
        protected long firstIdx;
        protected long frameSize;
        protected long nthValue;
        protected long size;
        protected long startOffset;

        public Decimal64NthValueOverRangeFrameFunction(
                long rangeLo,
                long rangeHi,
                Function arg,
                CairoConfiguration configuration,
                int timestampIdx,
                int n,
                int type
        ) {
            super(arg);
            frameLoBounded = rangeLo != Long.MIN_VALUE;
            maxDiff = frameLoBounded ? Math.abs(rangeLo) : Math.abs(rangeHi);
            minDiff = Math.abs(rangeHi);
            timestampIndex = timestampIdx;
            initialCapacity = configuration.getSqlWindowStorePageSize() / RECORD_SIZE;
            capacity = initialCapacity;
            MemoryARW mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                    configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
            try {
                startOffset = mem.appendAddressFor(capacity * RECORD_SIZE) - mem.getPageAddress(0);
            } catch (Throwable t) {
                Misc.free(mem);
                throw t;
            }
            memory = mem;
            firstIdx = 0;
            frameSize = 0;
            frameIncludesCurrentValue = rangeHi == 0;
            this.n = n;
            this.type = type;
        }

        @Override
        public void close() {
            super.close();
            memory.close();
        }

        @Override
        public void computeNext(Record record) {
            if (!frameLoBounded && frameSize >= n) {
                long nthIdx = (firstIdx + n - 1) % capacity;
                nthValue = memory.getLong(startOffset + nthIdx * RECORD_SIZE + Long.BYTES);
                return;
            }

            long timestamp = record.getTimestamp(timestampIndex);
            long d = arg.getDecimal64(record);
            long newFirstIdx = firstIdx;

            if (frameLoBounded) {
                for (long i = 0, nn = size; i < nn; i++) {
                    long idx = (firstIdx + i) % capacity;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    if (Math.abs(timestamp - ts) > maxDiff) {
                        if (frameSize > 0) {
                            frameSize--;
                        }
                        newFirstIdx = (idx + 1) % capacity;
                        size--;
                    } else {
                        break;
                    }
                }
            }
            firstIdx = newFirstIdx;

            if (size == capacity) {
                memoryDesc.reset(capacity, startOffset, size, firstIdx, freeList);
                expandRingBuffer(memory, memoryDesc, RECORD_SIZE);
                capacity = memoryDesc.capacity;
                startOffset = memoryDesc.startOffset;
                firstIdx = memoryDesc.firstIdx;
            }
            memory.putLong(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE, timestamp);
            memory.putLong(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE + Long.BYTES, d);
            size++;

            if (frameLoBounded) {
                for (long i = frameSize, nn = size; i < nn; i++) {
                    long idx = (firstIdx + i) % capacity;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    long diff = Math.abs(ts - timestamp);
                    if (diff <= maxDiff && diff >= minDiff) {
                        frameSize++;
                    } else {
                        break;
                    }
                }
            } else {
                for (long i = frameSize, nn = size; i < nn; i++) {
                    long idx = (firstIdx + i) % capacity;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    if (Math.abs(timestamp - ts) >= minDiff) {
                        frameSize++;
                    } else {
                        break;
                    }
                }
            }

            if (frameSize >= n) {
                long nthIdx = (firstIdx + n - 1) % capacity;
                nthValue = memory.getLong(startOffset + nthIdx * RECORD_SIZE + Long.BYTES);
            } else {
                nthValue = Decimals.DECIMAL64_NULL;
            }
        }

        @Override
        public long getDecimal64(Record rec) {
            return nthValue;
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
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putLong(spi.getAddress(recordOffset, columnIndex), nthValue);
        }

        @Override
        public void reopen() {
            nthValue = Decimals.DECIMAL64_NULL;
            capacity = initialCapacity;
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            frameSize = 0;
            size = 0;
            freeList.clear();
        }

        @Override
        public void reset() {
            super.reset();
            memory.close();
            freeList.clear();
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(',').val(n).val(')');
            sink.val(" over (");
            sink.val("range between ");
            if (frameLoBounded) {
                sink.val(maxDiff).val(" preceding");
            } else {
                sink.val("unbounded preceding");
            }
            sink.val(" and ");
            if (minDiff == 0) {
                sink.val("current row");
            } else {
                sink.val(minDiff).val(" preceding");
            }
            sink.val(')');
        }

        @Override
        public void toTop() {
            super.toTop();
            nthValue = Decimals.DECIMAL64_NULL;
            capacity = initialCapacity;
            memory.truncate();
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            frameSize = 0;
            size = 0;
            freeList.clear();
        }
    }

    public static class Decimal64NthValueOverRowsFrameFunction extends BaseWindowFunction implements Reopenable, WindowDecimal64Function {
        protected final MemoryARW buffer;
        protected final int bufferSize;
        protected final int excludeCount;
        protected final boolean frameIncludesCurrentValue;
        protected final int frameSize;
        protected final int n;
        protected final int type;
        protected long count = 0;
        protected int loIdx = 0;
        protected long nthValue;

        public Decimal64NthValueOverRowsFrameFunction(Function arg, long rowsLo, long rowsHi, MemoryARW memory, int n, int type) {
            super(arg);
            assert rowsLo > Long.MIN_VALUE;
            frameSize = (int) (rowsHi - rowsLo + (rowsHi < 0 ? 1 : 0));
            bufferSize = (int) Math.abs(rowsLo);
            excludeCount = rowsHi < 0 ? (int) Math.abs(rowsHi) : 0;
            frameIncludesCurrentValue = rowsHi == 0;
            this.buffer = memory;
            this.n = n;
            this.type = type;
            initBuffer();
        }

        @Override
        public void close() {
            super.close();
            buffer.close();
        }

        @Override
        public void computeNext(Record record) {
            long d = arg.getDecimal64(record);

            long effectiveCount = Math.min(count, bufferSize);
            long currentFrameElements;
            if (frameIncludesCurrentValue) {
                currentFrameElements = Math.min(effectiveCount, frameSize);
            } else {
                currentFrameElements = Math.clamp(count + 1 - excludeCount, 0, frameSize);
            }

            if (currentFrameElements > 0 || (count == 0 && frameIncludesCurrentValue)) {
                long frameStartIdx = (loIdx + bufferSize - effectiveCount) % bufferSize;
                if (frameIncludesCurrentValue) {
                    if (n <= currentFrameElements) {
                        long nthIdx = (frameStartIdx + n - 1) % bufferSize;
                        nthValue = buffer.getLong(nthIdx * Long.BYTES);
                    } else if (n == currentFrameElements + 1) {
                        nthValue = d;
                    } else {
                        nthValue = Decimals.DECIMAL64_NULL;
                    }
                } else {
                    if (n <= currentFrameElements) {
                        long nthIdx = (frameStartIdx + n - 1) % bufferSize;
                        nthValue = buffer.getLong(nthIdx * Long.BYTES);
                    } else {
                        nthValue = Decimals.DECIMAL64_NULL;
                    }
                }
            } else {
                nthValue = Decimals.DECIMAL64_NULL;
            }

            count = Math.min(count + 1, bufferSize);
            buffer.putLong((long) loIdx * Long.BYTES, d);
            loIdx = (loIdx + 1) % bufferSize;
        }

        @Override
        public long getDecimal64(Record rec) {
            return nthValue;
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
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putLong(spi.getAddress(recordOffset, columnIndex), nthValue);
        }

        @Override
        public void reopen() {
            nthValue = Decimals.DECIMAL64_NULL;
            loIdx = 0;
            initBuffer();
            count = 0;
        }

        @Override
        public void reset() {
            super.reset();
            buffer.close();
            nthValue = Decimals.DECIMAL64_NULL;
            loIdx = 0;
            count = 0;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(',').val(n).val(')');
            sink.val(" over (");
            sink.val(" rows between ");
            sink.val(bufferSize);
            sink.val(" preceding and ");
            if (frameIncludesCurrentValue) {
                sink.val("current row");
            } else {
                sink.val(excludeCount).val(" preceding");
            }
            sink.val(')');
        }

        @Override
        public void toTop() {
            super.toTop();
            nthValue = Decimals.DECIMAL64_NULL;
            loIdx = 0;
            initBuffer();
            count = 0;
        }

        private void initBuffer() {
            for (int i = 0; i < bufferSize; i++) {
                buffer.putLong((long) i * Long.BYTES, Decimals.DECIMAL64_NULL);
            }
        }
    }

    public static class Decimal64NthValueOverRowsFrameUnboundedFunction extends BaseWindowFunction implements Reopenable, WindowDecimal64Function {
        protected final int bufferSize;
        protected final int n;
        protected final int type;
        protected long lockedValue = Decimals.DECIMAL64_NULL;
        protected long nthValue = Decimals.DECIMAL64_NULL;
        protected long totalCount = 0;

        public Decimal64NthValueOverRowsFrameUnboundedFunction(Function arg, long rowsHi, int n, int type) {
            super(arg);
            assert rowsHi < 0 && rowsHi != Long.MIN_VALUE;
            this.bufferSize = (int) Math.abs(rowsHi);
            this.n = n;
            this.type = type;
        }

        @Override
        public void computeNext(Record record) {
            long d = arg.getDecimal64(record);
            totalCount++;
            if (totalCount == n) {
                lockedValue = d;
            }
            nthValue = totalCount >= (long) n + bufferSize ? lockedValue : Decimals.DECIMAL64_NULL;
        }

        @Override
        public long getDecimal64(Record rec) {
            return nthValue;
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
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putLong(spi.getAddress(recordOffset, columnIndex), nthValue);
        }

        @Override
        public void reopen() {
            nthValue = Decimals.DECIMAL64_NULL;
            lockedValue = Decimals.DECIMAL64_NULL;
            totalCount = 0;
        }

        @Override
        public void reset() {
            super.reset();
            nthValue = Decimals.DECIMAL64_NULL;
            lockedValue = Decimals.DECIMAL64_NULL;
            totalCount = 0;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(',').val(n).val(')');
            sink.val(" over (");
            sink.val(" rows between unbounded preceding and ");
            sink.val(bufferSize).val(" preceding");
            sink.val(')');
        }

        @Override
        public void toTop() {
            super.toTop();
            nthValue = Decimals.DECIMAL64_NULL;
            lockedValue = Decimals.DECIMAL64_NULL;
            totalCount = 0;
        }
    }

    static class Decimal64NthValueOverUnboundedPartitionFrameFunction extends BasePartitionedWindowFunction implements WindowDecimal64Function {

        protected final boolean isRange;
        protected final int n;
        protected final int type;
        protected long value;

        public Decimal64NthValueOverUnboundedPartitionFrameFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, int n, boolean isRange, int type) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.n = n;
            this.isRange = isRange;
            this.type = type;
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mapValue = key.createValue();

            if (mapValue.isNew()) {
                if (n == 1) {
                    long d = arg.getDecimal64(record);
                    mapValue.putLong(0, d);
                    value = d;
                } else {
                    mapValue.putLong(0, Decimals.DECIMAL64_NULL);
                    value = Decimals.DECIMAL64_NULL;
                }
                mapValue.putLong(1, 1);
            } else {
                long count = mapValue.getLong(1);
                if (count >= n) {
                    value = mapValue.getLong(0);
                } else {
                    count++;
                    mapValue.putLong(1, count);
                    if (count == n) {
                        long d = arg.getDecimal64(record);
                        mapValue.putLong(0, d);
                        value = d;
                    } else {
                        value = Decimals.DECIMAL64_NULL;
                    }
                }
            }
        }

        @Override
        public long getDecimal64(Record rec) {
            return value;
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
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putLong(spi.getAddress(recordOffset, columnIndex), value);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(',').val(n).val(')');
            sink.val(" over (");
            sink.val("partition by ");
            sink.val(partitionByRecord.getFunctions());
            sink.val(isRange ? " range" : " rows");
            sink.val(" between unbounded preceding and current row)");
        }
    }

    public static class Decimal64NthValueOverUnboundedRowsFrameFunction extends BaseWindowFunction implements WindowDecimal64Function {
        protected final int n;
        protected final int type;
        protected long count;
        protected boolean isFound;
        protected long value = Decimals.DECIMAL64_NULL;

        public Decimal64NthValueOverUnboundedRowsFrameFunction(Function arg, int n, int type) {
            super(arg);
            this.n = n;
            this.type = type;
        }

        @Override
        public void computeNext(Record record) {
            if (!isFound) {
                count++;
                if (count == n) {
                    value = arg.getDecimal64(record);
                    isFound = true;
                }
            }
        }

        @Override
        public long getDecimal64(Record rec) {
            return value;
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
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putLong(spi.getAddress(recordOffset, columnIndex), value);
        }

        @Override
        public void reset() {
            super.reset();
            count = 0;
            isFound = false;
            value = Decimals.DECIMAL64_NULL;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(',').val(n).val(')');
            sink.val(" over (rows between unbounded preceding and current row)");
        }

        @Override
        public void toTop() {
            super.toTop();
            count = 0;
            isFound = false;
            value = Decimals.DECIMAL64_NULL;
        }
    }

    public static class Decimal64NthValueOverWholeResultSetFunction extends BaseWindowFunction implements WindowDecimal64Function {
        protected final int n;
        protected final int type;
        protected long count;
        protected boolean isFound;
        protected long value = Decimals.DECIMAL64_NULL;

        public Decimal64NthValueOverWholeResultSetFunction(Function arg, int n, int type) {
            super(arg);
            this.n = n;
            this.type = type;
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public int getPassCount() {
            return WindowFunction.TWO_PASS;
        }

        @Override
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            if (!isFound) {
                count++;
                if (count == n) {
                    value = arg.getDecimal64(record);
                    isFound = true;
                }
            }
        }

        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            Unsafe.putLong(spi.getAddress(recordOffset, columnIndex), value);
        }

        @Override
        public void reset() {
            super.reset();
            clearState();
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(',').val(n).val(')');
            sink.val(" over ()");
        }

        @Override
        public void toTop() {
            super.toTop();
            clearState();
        }

        private void clearState() {
            count = 0;
            isFound = false;
            value = Decimals.DECIMAL64_NULL;
        }
    }

    public static class Decimal8NthValueOverPartitionRangeFrameFunction extends BasePartitionedWindowFunction implements WindowDecimal8Function {

        protected static final int RECORD_SIZE = Long.BYTES + Byte.BYTES;
        protected final boolean frameIncludesCurrentValue;
        protected final boolean frameLoBounded;
        protected final LongList freeList = new LongList();
        protected final int initialBufferSize;
        protected final long maxDiff;
        protected final MemoryARW memory;
        protected final RingBufferDesc memoryDesc = new RingBufferDesc();
        protected final long minDiff;
        protected final int n;
        protected final int timestampIndex;
        protected final int type;
        protected byte nthValue;

        public Decimal8NthValueOverPartitionRangeFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rangeLo,
                long rangeHi,
                Function arg,
                MemoryARW memory,
                int initialBufferSize,
                int timestampIdx,
                int n,
                int type
        ) {
            super(map, partitionByRecord, partitionBySink, arg);
            frameLoBounded = rangeLo != Long.MIN_VALUE;
            maxDiff = frameLoBounded ? Math.abs(rangeLo) : Math.abs(rangeHi);
            minDiff = Math.abs(rangeHi);
            this.memory = memory;
            this.initialBufferSize = initialBufferSize;
            this.timestampIndex = timestampIdx;
            this.n = n;
            this.frameIncludesCurrentValue = rangeHi == 0;
            this.type = type;
        }

        @Override
        public void close() {
            super.close();
            memory.close();
            freeList.clear();
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mapValue = key.createValue();

            long frameSize;
            long startOffset;
            long size;
            long capacity;
            long firstIdx;
            long timestamp = record.getTimestamp(timestampIndex);
            byte b = arg.getDecimal8(record);

            if (mapValue.isNew()) {
                capacity = initialBufferSize;
                startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
                firstIdx = 0;
                memory.putLong(startOffset, timestamp);
                memory.putByte(startOffset + Long.BYTES, b);
                size = 1;
                if (frameIncludesCurrentValue) {
                    frameSize = 1;
                    nthValue = n == 1 ? b : Decimals.DECIMAL8_NULL;
                } else {
                    frameSize = 0;
                    nthValue = Decimals.DECIMAL8_NULL;
                }
            } else {
                frameSize = mapValue.getLong(0);
                startOffset = mapValue.getLong(1);
                size = mapValue.getLong(2);
                capacity = mapValue.getLong(3);
                firstIdx = mapValue.getLong(4);

                if (!frameLoBounded && frameSize >= n) {
                    long nthIdx = (firstIdx + n - 1) % capacity;
                    nthValue = memory.getByte(startOffset + nthIdx * RECORD_SIZE + Long.BYTES);
                    return;
                }

                long newFirstIdx = firstIdx;
                if (frameLoBounded) {
                    for (long i = 0, nn = size; i < nn; i++) {
                        long idx = (firstIdx + i) % capacity;
                        long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                        if (Math.abs(timestamp - ts) > maxDiff) {
                            if (frameSize > 0) {
                                frameSize--;
                            }
                            newFirstIdx = (idx + 1) % capacity;
                            size--;
                        } else {
                            break;
                        }
                    }
                }
                firstIdx = newFirstIdx;

                if (size == capacity) {
                    memoryDesc.reset(capacity, startOffset, size, firstIdx, freeList);
                    expandRingBuffer(memory, memoryDesc, RECORD_SIZE);
                    capacity = memoryDesc.capacity;
                    startOffset = memoryDesc.startOffset;
                    firstIdx = memoryDesc.firstIdx;
                }
                memory.putLong(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE, timestamp);
                memory.putByte(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE + Long.BYTES, b);
                size++;

                if (frameLoBounded) {
                    for (long i = frameSize; i < size; i++) {
                        long idx = (firstIdx + i) % capacity;
                        long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                        long diff = Math.abs(ts - timestamp);
                        if (diff <= maxDiff && diff >= minDiff) {
                            frameSize++;
                        } else {
                            break;
                        }
                    }
                } else {
                    for (long i = frameSize; i < size; i++) {
                        long idx = (firstIdx + i) % capacity;
                        long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                        if (Math.abs(timestamp - ts) >= minDiff) {
                            frameSize++;
                        } else {
                            break;
                        }
                    }
                }

                if (frameSize >= n) {
                    long nthIdx = (firstIdx + n - 1) % capacity;
                    nthValue = memory.getByte(startOffset + nthIdx * RECORD_SIZE + Long.BYTES);
                } else {
                    nthValue = Decimals.DECIMAL8_NULL;
                }
            }

            mapValue.putLong(0, frameSize);
            mapValue.putLong(1, startOffset);
            mapValue.putLong(2, size);
            mapValue.putLong(3, capacity);
            mapValue.putLong(4, firstIdx);
        }

        @Override
        public byte getDecimal8(Record rec) {
            return nthValue;
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
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putByte(spi.getAddress(recordOffset, columnIndex), nthValue);
        }

        @Override
        public void reopen() {
            super.reopen();
            freeList.clear();
            nthValue = Decimals.DECIMAL8_NULL;
        }

        @Override
        public void reset() {
            super.reset();
            memory.close();
            freeList.clear();
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(',').val(n).val(')');
            sink.val(" over (");
            sink.val("partition by ");
            sink.val(partitionByRecord.getFunctions());
            sink.val(" range between ");
            if (frameLoBounded) {
                sink.val(maxDiff).val(" preceding");
            } else {
                sink.val("unbounded preceding");
            }
            sink.val(" and ");
            if (minDiff == 0) {
                sink.val("current row");
            } else {
                sink.val(minDiff).val(" preceding");
            }
            sink.val(')');
        }

        @Override
        public void toTop() {
            super.toTop();
            memory.truncate();
            freeList.clear();
        }
    }

    public static class Decimal8NthValueOverPartitionRowsFrameFunction extends BasePartitionedWindowFunction implements WindowDecimal8Function {

        protected final int bufferSize;
        protected final int excludeCount;
        protected final boolean frameIncludesCurrentValue;
        protected final int frameSize;
        protected final MemoryARW memory;
        protected final int n;
        protected final int type;
        protected byte nthValue;

        public Decimal8NthValueOverPartitionRowsFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rowsLo,
                long rowsHi,
                Function arg,
                MemoryARW memory,
                int n,
                int type
        ) {
            super(map, partitionByRecord, partitionBySink, arg);
            assert rowsLo > Long.MIN_VALUE;
            frameSize = (int) (rowsHi - rowsLo + (rowsHi < 0 ? 1 : 0));
            bufferSize = (int) Math.abs(rowsLo);
            this.excludeCount = rowsHi < 0 ? (int) Math.abs(rowsHi) : 0;
            this.frameIncludesCurrentValue = rowsHi == 0;
            this.memory = memory;
            this.n = n;
            this.type = type;
        }

        @Override
        public void close() {
            super.close();
            memory.close();
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mapValue = key.createValue();

            byte b = arg.getDecimal8(record);

            long loIdx;
            long startOffset;
            long count;

            if (mapValue.isNew()) {
                loIdx = 0;
                count = 0;
                startOffset = memory.appendAddressFor((long) bufferSize * Byte.BYTES) - memory.getPageAddress(0);
                mapValue.putLong(1, startOffset);
                for (int i = 0; i < bufferSize; i++) {
                    memory.putByte(startOffset + (long) i * Byte.BYTES, Decimals.DECIMAL8_NULL);
                }
            } else {
                loIdx = mapValue.getLong(0);
                startOffset = mapValue.getLong(1);
                count = mapValue.getLong(2);
            }

            long effectiveCount = Math.min(count, bufferSize);
            long currentFrameSize;
            if (frameIncludesCurrentValue) {
                currentFrameSize = Math.min(effectiveCount, frameSize);
            } else {
                currentFrameSize = Math.clamp(count + 1 - excludeCount, 0, frameSize);
            }

            if (currentFrameSize > 0 || (count == 0 && frameIncludesCurrentValue)) {
                long frameStartIdx = (loIdx + bufferSize - effectiveCount) % bufferSize;
                if (frameIncludesCurrentValue) {
                    if (n <= currentFrameSize) {
                        long nthIdx = (frameStartIdx + n - 1) % bufferSize;
                        nthValue = memory.getByte(startOffset + nthIdx * Byte.BYTES);
                    } else if (n == currentFrameSize + 1) {
                        nthValue = b;
                    } else {
                        nthValue = Decimals.DECIMAL8_NULL;
                    }
                } else {
                    if (n <= currentFrameSize) {
                        long nthIdx = (frameStartIdx + n - 1) % bufferSize;
                        nthValue = memory.getByte(startOffset + nthIdx * Byte.BYTES);
                    } else {
                        nthValue = Decimals.DECIMAL8_NULL;
                    }
                }
            } else {
                nthValue = Decimals.DECIMAL8_NULL;
            }

            count = Math.min(count + 1, bufferSize);
            mapValue.putLong(0, (loIdx + 1) % bufferSize);
            mapValue.putLong(2, count);
            memory.putByte(startOffset + loIdx * Byte.BYTES, b);
        }

        @Override
        public byte getDecimal8(Record rec) {
            return nthValue;
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
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putByte(spi.getAddress(recordOffset, columnIndex), nthValue);
        }

        @Override
        public void reopen() {
            super.reopen();
        }

        @Override
        public void reset() {
            super.reset();
            memory.close();
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(',').val(n).val(')');
            sink.val(" over (");
            sink.val("partition by ");
            sink.val(partitionByRecord.getFunctions());
            sink.val(" rows between ");
            sink.val(bufferSize);
            sink.val(" preceding and ");
            if (frameIncludesCurrentValue) {
                sink.val("current row");
            } else {
                sink.val(excludeCount).val(" preceding");
            }
            sink.val(')');
        }

        @Override
        public void toTop() {
            super.toTop();
            memory.truncate();
        }
    }

    public static class Decimal8NthValueOverPartitionRowsFrameUnboundedFunction extends BasePartitionedWindowFunction implements WindowDecimal8Function {

        protected final int bufferSize;
        protected final int n;
        protected final int type;
        protected byte nthValue;

        public Decimal8NthValueOverPartitionRowsFrameUnboundedFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rowsHi,
                Function arg,
                int n,
                int type
        ) {
            super(map, partitionByRecord, partitionBySink, arg);
            assert rowsHi < 0 && rowsHi != Long.MIN_VALUE;
            this.bufferSize = (int) Math.abs(rowsHi);
            this.n = n;
            this.type = type;
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mapValue = key.createValue();

            byte b = arg.getDecimal8(record);
            long count;
            if (mapValue.isNew()) {
                count = 0;
                mapValue.putLong(1, Decimals.DECIMAL8_NULL);
            } else {
                count = mapValue.getLong(0);
            }
            count++;
            if (count == n) {
                mapValue.putLong(1, b);
            }
            if (count >= (long) n + bufferSize) {
                nthValue = (byte) mapValue.getLong(1);
            } else {
                nthValue = Decimals.DECIMAL8_NULL;
            }
            mapValue.putLong(0, count);
        }

        @Override
        public byte getDecimal8(Record rec) {
            return nthValue;
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
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putByte(spi.getAddress(recordOffset, columnIndex), nthValue);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(',').val(n).val(')');
            sink.val(" over (");
            sink.val("partition by ");
            sink.val(partitionByRecord.getFunctions());
            sink.val(" rows between unbounded preceding and ");
            sink.val(bufferSize).val(" preceding");
            sink.val(')');
        }
    }

    public static class Decimal8NthValueOverRangeFrameFunction extends BaseWindowFunction implements Reopenable, WindowDecimal8Function {
        protected static final int RECORD_SIZE = Long.BYTES + Byte.BYTES;
        protected final boolean frameIncludesCurrentValue;
        protected final boolean frameLoBounded;
        protected final LongList freeList = new LongList();
        protected final long initialCapacity;
        protected final long maxDiff;
        protected final MemoryARW memory;
        protected final RingBufferDesc memoryDesc = new RingBufferDesc();
        protected final long minDiff;
        protected final int n;
        protected final int timestampIndex;
        protected final int type;
        protected long capacity;
        protected long firstIdx;
        protected long frameSize;
        protected byte nthValue;
        protected long size;
        protected long startOffset;

        public Decimal8NthValueOverRangeFrameFunction(
                long rangeLo,
                long rangeHi,
                Function arg,
                CairoConfiguration configuration,
                int timestampIdx,
                int n,
                int type
        ) {
            super(arg);
            frameLoBounded = rangeLo != Long.MIN_VALUE;
            maxDiff = frameLoBounded ? Math.abs(rangeLo) : Math.abs(rangeHi);
            minDiff = Math.abs(rangeHi);
            timestampIndex = timestampIdx;
            initialCapacity = configuration.getSqlWindowStorePageSize() / RECORD_SIZE;
            capacity = initialCapacity;
            MemoryARW mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                    configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
            try {
                startOffset = mem.appendAddressFor(capacity * RECORD_SIZE) - mem.getPageAddress(0);
            } catch (Throwable t) {
                Misc.free(mem);
                throw t;
            }
            memory = mem;
            firstIdx = 0;
            frameSize = 0;
            frameIncludesCurrentValue = rangeHi == 0;
            this.n = n;
            this.type = type;
        }

        @Override
        public void close() {
            super.close();
            memory.close();
        }

        @Override
        public void computeNext(Record record) {
            if (!frameLoBounded && frameSize >= n) {
                long nthIdx = (firstIdx + n - 1) % capacity;
                nthValue = memory.getByte(startOffset + nthIdx * RECORD_SIZE + Long.BYTES);
                return;
            }

            long timestamp = record.getTimestamp(timestampIndex);
            byte b = arg.getDecimal8(record);
            long newFirstIdx = firstIdx;

            if (frameLoBounded) {
                for (long i = 0, nn = size; i < nn; i++) {
                    long idx = (firstIdx + i) % capacity;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    if (Math.abs(timestamp - ts) > maxDiff) {
                        if (frameSize > 0) {
                            frameSize--;
                        }
                        newFirstIdx = (idx + 1) % capacity;
                        size--;
                    } else {
                        break;
                    }
                }
            }
            firstIdx = newFirstIdx;

            if (size == capacity) {
                memoryDesc.reset(capacity, startOffset, size, firstIdx, freeList);
                expandRingBuffer(memory, memoryDesc, RECORD_SIZE);
                capacity = memoryDesc.capacity;
                startOffset = memoryDesc.startOffset;
                firstIdx = memoryDesc.firstIdx;
            }
            memory.putLong(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE, timestamp);
            memory.putByte(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE + Long.BYTES, b);
            size++;

            if (frameLoBounded) {
                for (long i = frameSize, nn = size; i < nn; i++) {
                    long idx = (firstIdx + i) % capacity;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    long diff = Math.abs(ts - timestamp);
                    if (diff <= maxDiff && diff >= minDiff) {
                        frameSize++;
                    } else {
                        break;
                    }
                }
            } else {
                for (long i = frameSize, nn = size; i < nn; i++) {
                    long idx = (firstIdx + i) % capacity;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    if (Math.abs(timestamp - ts) >= minDiff) {
                        frameSize++;
                    } else {
                        break;
                    }
                }
            }

            if (frameSize >= n) {
                long nthIdx = (firstIdx + n - 1) % capacity;
                nthValue = memory.getByte(startOffset + nthIdx * RECORD_SIZE + Long.BYTES);
            } else {
                nthValue = Decimals.DECIMAL8_NULL;
            }
        }

        @Override
        public byte getDecimal8(Record rec) {
            return nthValue;
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
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putByte(spi.getAddress(recordOffset, columnIndex), nthValue);
        }

        @Override
        public void reopen() {
            nthValue = Decimals.DECIMAL8_NULL;
            capacity = initialCapacity;
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            frameSize = 0;
            size = 0;
            freeList.clear();
        }

        @Override
        public void reset() {
            super.reset();
            memory.close();
            freeList.clear();
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(',').val(n).val(')');
            sink.val(" over (");
            sink.val("range between ");
            if (frameLoBounded) {
                sink.val(maxDiff).val(" preceding");
            } else {
                sink.val("unbounded preceding");
            }
            sink.val(" and ");
            if (minDiff == 0) {
                sink.val("current row");
            } else {
                sink.val(minDiff).val(" preceding");
            }
            sink.val(')');
        }

        @Override
        public void toTop() {
            super.toTop();
            nthValue = Decimals.DECIMAL8_NULL;
            capacity = initialCapacity;
            memory.truncate();
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            frameSize = 0;
            size = 0;
            freeList.clear();
        }
    }

    public static class Decimal8NthValueOverRowsFrameFunction extends BaseWindowFunction implements Reopenable, WindowDecimal8Function {
        protected final MemoryARW buffer;
        protected final int bufferSize;
        protected final int excludeCount;
        protected final boolean frameIncludesCurrentValue;
        protected final int frameSize;
        protected final int n;
        protected final int type;
        protected long count = 0;
        protected int loIdx = 0;
        protected byte nthValue;

        public Decimal8NthValueOverRowsFrameFunction(Function arg, long rowsLo, long rowsHi, MemoryARW memory, int n, int type) {
            super(arg);
            assert rowsLo > Long.MIN_VALUE;
            frameSize = (int) (rowsHi - rowsLo + (rowsHi < 0 ? 1 : 0));
            bufferSize = (int) Math.abs(rowsLo);
            excludeCount = rowsHi < 0 ? (int) Math.abs(rowsHi) : 0;
            frameIncludesCurrentValue = rowsHi == 0;
            this.buffer = memory;
            this.n = n;
            this.type = type;
            initBuffer();
        }

        @Override
        public void close() {
            super.close();
            buffer.close();
        }

        @Override
        public void computeNext(Record record) {
            byte b = arg.getDecimal8(record);

            long effectiveCount = Math.min(count, bufferSize);
            long currentFrameElements;
            if (frameIncludesCurrentValue) {
                currentFrameElements = Math.min(effectiveCount, frameSize);
            } else {
                currentFrameElements = Math.clamp(count + 1 - excludeCount, 0, frameSize);
            }

            if (currentFrameElements > 0 || (count == 0 && frameIncludesCurrentValue)) {
                long frameStartIdx = (loIdx + bufferSize - effectiveCount) % bufferSize;
                if (frameIncludesCurrentValue) {
                    if (n <= currentFrameElements) {
                        long nthIdx = (frameStartIdx + n - 1) % bufferSize;
                        nthValue = buffer.getByte(nthIdx * Byte.BYTES);
                    } else if (n == currentFrameElements + 1) {
                        nthValue = b;
                    } else {
                        nthValue = Decimals.DECIMAL8_NULL;
                    }
                } else {
                    if (n <= currentFrameElements) {
                        long nthIdx = (frameStartIdx + n - 1) % bufferSize;
                        nthValue = buffer.getByte(nthIdx * Byte.BYTES);
                    } else {
                        nthValue = Decimals.DECIMAL8_NULL;
                    }
                }
            } else {
                nthValue = Decimals.DECIMAL8_NULL;
            }

            count = Math.min(count + 1, bufferSize);
            buffer.putByte((long) loIdx * Byte.BYTES, b);
            loIdx = (loIdx + 1) % bufferSize;
        }

        @Override
        public byte getDecimal8(Record rec) {
            return nthValue;
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
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putByte(spi.getAddress(recordOffset, columnIndex), nthValue);
        }

        @Override
        public void reopen() {
            nthValue = Decimals.DECIMAL8_NULL;
            loIdx = 0;
            initBuffer();
            count = 0;
        }

        @Override
        public void reset() {
            super.reset();
            buffer.close();
            nthValue = Decimals.DECIMAL8_NULL;
            loIdx = 0;
            count = 0;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(',').val(n).val(')');
            sink.val(" over (");
            sink.val(" rows between ");
            sink.val(bufferSize);
            sink.val(" preceding and ");
            if (frameIncludesCurrentValue) {
                sink.val("current row");
            } else {
                sink.val(excludeCount).val(" preceding");
            }
            sink.val(')');
        }

        @Override
        public void toTop() {
            super.toTop();
            nthValue = Decimals.DECIMAL8_NULL;
            loIdx = 0;
            initBuffer();
            count = 0;
        }

        private void initBuffer() {
            for (int i = 0; i < bufferSize; i++) {
                buffer.putByte((long) i * Byte.BYTES, Decimals.DECIMAL8_NULL);
            }
        }
    }

    public static class Decimal8NthValueOverRowsFrameUnboundedFunction extends BaseWindowFunction implements Reopenable, WindowDecimal8Function {
        protected final int bufferSize;
        protected final int n;
        protected final int type;
        protected byte lockedValue = Decimals.DECIMAL8_NULL;
        protected byte nthValue = Decimals.DECIMAL8_NULL;
        protected long totalCount = 0;

        public Decimal8NthValueOverRowsFrameUnboundedFunction(Function arg, long rowsHi, int n, int type) {
            super(arg);
            assert rowsHi < 0 && rowsHi != Long.MIN_VALUE;
            this.bufferSize = (int) Math.abs(rowsHi);
            this.n = n;
            this.type = type;
        }

        @Override
        public void computeNext(Record record) {
            byte b = arg.getDecimal8(record);
            totalCount++;
            if (totalCount == n) {
                lockedValue = b;
            }
            nthValue = totalCount >= (long) n + bufferSize ? lockedValue : Decimals.DECIMAL8_NULL;
        }

        @Override
        public byte getDecimal8(Record rec) {
            return nthValue;
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
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putByte(spi.getAddress(recordOffset, columnIndex), nthValue);
        }

        @Override
        public void reopen() {
            nthValue = Decimals.DECIMAL8_NULL;
            lockedValue = Decimals.DECIMAL8_NULL;
            totalCount = 0;
        }

        @Override
        public void reset() {
            super.reset();
            nthValue = Decimals.DECIMAL8_NULL;
            lockedValue = Decimals.DECIMAL8_NULL;
            totalCount = 0;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(',').val(n).val(')');
            sink.val(" over (");
            sink.val(" rows between unbounded preceding and ");
            sink.val(bufferSize).val(" preceding");
            sink.val(')');
        }

        @Override
        public void toTop() {
            super.toTop();
            nthValue = Decimals.DECIMAL8_NULL;
            lockedValue = Decimals.DECIMAL8_NULL;
            totalCount = 0;
        }
    }

    static class Decimal8NthValueOverCurrentRowFunction extends BaseWindowFunction implements WindowDecimal8Function {

        private final int n;
        private final int type;
        private byte value;

        Decimal8NthValueOverCurrentRowFunction(Function arg, int n, int type) {
            super(arg);
            this.n = n;
            this.type = type;
        }

        @Override
        public void computeNext(Record record) {
            value = n == 1 ? arg.getDecimal8(record) : Decimals.DECIMAL8_NULL;
        }

        @Override
        public byte getDecimal8(Record rec) {
            return value;
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
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putByte(spi.getAddress(recordOffset, columnIndex), value);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(',').val(n).val(')');
            sink.val(" over (rows between current row and current row)");
        }
    }

    static class Decimal8NthValueOverPartitionFunction extends BasePartitionedWindowFunction implements WindowDecimal8Function {

        private final int n;
        private final int type;

        public Decimal8NthValueOverPartitionFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, int n, int type) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.n = n;
            this.type = type;
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public int getPassCount() {
            return WindowFunction.TWO_PASS;
        }

        @Override
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.createValue();

            if (value.isNew()) {
                if (n == 1) {
                    value.putLong(0, arg.getDecimal8(record));
                } else {
                    value.putLong(0, Decimals.DECIMAL8_NULL);
                }
                value.putLong(1, 1);
            } else {
                long count = value.getLong(1) + 1;
                value.putLong(1, count);
                if (count == n) {
                    value.putLong(0, arg.getDecimal8(record));
                }
            }
        }

        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.findValue();
            assert value != null;
            Unsafe.putByte(spi.getAddress(recordOffset, columnIndex), (byte) value.getLong(0));
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(',').val(n).val(')');
            sink.val(" over (");
            sink.val("partition by ");
            sink.val(partitionByRecord.getFunctions());
            sink.val(')');
        }
    }

    static class Decimal8NthValueOverUnboundedPartitionFrameFunction extends BasePartitionedWindowFunction implements WindowDecimal8Function {

        protected final boolean isRange;
        protected final int n;
        protected final int type;
        protected byte value;

        public Decimal8NthValueOverUnboundedPartitionFrameFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, int n, boolean isRange, int type) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.n = n;
            this.isRange = isRange;
            this.type = type;
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mapValue = key.createValue();

            if (mapValue.isNew()) {
                if (n == 1) {
                    byte b = arg.getDecimal8(record);
                    mapValue.putLong(0, b);
                    value = b;
                } else {
                    mapValue.putLong(0, Decimals.DECIMAL8_NULL);
                    value = Decimals.DECIMAL8_NULL;
                }
                mapValue.putLong(1, 1);
            } else {
                long count = mapValue.getLong(1);
                if (count >= n) {
                    value = (byte) mapValue.getLong(0);
                } else {
                    count++;
                    mapValue.putLong(1, count);
                    if (count == n) {
                        byte b = arg.getDecimal8(record);
                        mapValue.putLong(0, b);
                        value = b;
                    } else {
                        value = Decimals.DECIMAL8_NULL;
                    }
                }
            }
        }

        @Override
        public byte getDecimal8(Record rec) {
            return value;
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
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putByte(spi.getAddress(recordOffset, columnIndex), value);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(',').val(n).val(')');
            sink.val(" over (");
            sink.val("partition by ");
            sink.val(partitionByRecord.getFunctions());
            sink.val(isRange ? " range" : " rows");
            sink.val(" between unbounded preceding and current row)");
        }
    }

    public static class Decimal8NthValueOverUnboundedRowsFrameFunction extends BaseWindowFunction implements WindowDecimal8Function {
        protected final int n;
        protected final int type;
        protected long count;
        protected boolean isFound;
        protected byte value = Decimals.DECIMAL8_NULL;

        public Decimal8NthValueOverUnboundedRowsFrameFunction(Function arg, int n, int type) {
            super(arg);
            this.n = n;
            this.type = type;
        }

        @Override
        public void computeNext(Record record) {
            if (!isFound) {
                count++;
                if (count == n) {
                    value = arg.getDecimal8(record);
                    isFound = true;
                }
            }
        }

        @Override
        public byte getDecimal8(Record rec) {
            return value;
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
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putByte(spi.getAddress(recordOffset, columnIndex), value);
        }

        @Override
        public void reset() {
            super.reset();
            count = 0;
            isFound = false;
            value = Decimals.DECIMAL8_NULL;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(',').val(n).val(')');
            sink.val(" over (rows between unbounded preceding and current row)");
        }

        @Override
        public void toTop() {
            super.toTop();
            count = 0;
            isFound = false;
            value = Decimals.DECIMAL8_NULL;
        }
    }

    public static class Decimal8NthValueOverWholeResultSetFunction extends BaseWindowFunction implements WindowDecimal8Function {
        protected final int n;
        protected final int type;
        protected long count;
        protected boolean isFound;
        protected byte value = Decimals.DECIMAL8_NULL;

        public Decimal8NthValueOverWholeResultSetFunction(Function arg, int n, int type) {
            super(arg);
            this.n = n;
            this.type = type;
        }

        @Override
        public byte getDecimal8(Record rec) {
            return value;
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public int getPassCount() {
            return WindowFunction.TWO_PASS;
        }

        @Override
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            if (!isFound) {
                count++;
                if (count == n) {
                    value = arg.getDecimal8(record);
                    isFound = true;
                }
            }
        }

        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            Unsafe.putByte(spi.getAddress(recordOffset, columnIndex), value);
        }

        @Override
        public void reset() {
            super.reset();
            clearState();
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(',').val(n).val(')');
            sink.val(" over ()");
        }

        @Override
        public void toTop() {
            super.toTop();
            clearState();
        }

        private void clearState() {
            count = 0;
            isFound = false;
            value = Decimals.DECIMAL8_NULL;
        }
    }

    private Function newInstanceDecimal8(
            int position,
            ObjList<Function> args,
            CairoConfiguration configuration,
            WindowContext windowContext,
            int n,
            int argType
    ) throws SqlException {
        long rowsLo = windowContext.getRowsLo();
        long rowsHi = windowContext.getRowsHi();
        if (windowContext.getFramingMode() == WindowExpression.FRAMING_ROWS) {
            if (rowsLo != Long.MIN_VALUE && Math.abs(rowsLo) > Integer.MAX_VALUE) {
                throw SqlException.$(windowContext.getRowsLoKindPos(), "frame start exceeds maximum supported size");
            }
            if (rowsHi != Long.MAX_VALUE && Math.abs(rowsHi) > Integer.MAX_VALUE) {
                throw SqlException.$(windowContext.getRowsHiKindPos(), "frame end exceeds maximum supported size");
            }
        }

        int framingMode = windowContext.getFramingMode();
        RecordSink partitionBySink = windowContext.getPartitionBySink();
        ColumnTypes partitionByKeyTypes = windowContext.getPartitionByKeyTypes();
        VirtualRecord partitionByRecord = windowContext.getPartitionByRecord();

        if (partitionByRecord != null) {
            if (framingMode == WindowExpression.FRAMING_RANGE) {
                if (windowContext.isDefaultFrame() && (!windowContext.isOrdered() || windowContext.getRowsHi() == Long.MAX_VALUE)) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, NTH_VALUE_DECIMAL64_TYPES);
                    try {
                        return new Decimal8NthValueOverPartitionFunction(map, partitionByRecord, partitionBySink, args.get(0), n, argType);
                    } catch (Throwable t) {
                        Misc.free(map);
                        throw t;
                    }
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, NTH_VALUE_DECIMAL64_TYPES);
                    try {
                        return new Decimal8NthValueOverUnboundedPartitionFrameFunction(map, partitionByRecord, partitionBySink, args.get(0), n, true, argType);
                    } catch (Throwable t) {
                        Misc.free(map);
                        throw t;
                    }
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    ArrayColumnTypes columnTypes = new ArrayColumnTypes();
                    columnTypes.add(ColumnType.LONG);
                    columnTypes.add(ColumnType.LONG);
                    columnTypes.add(ColumnType.LONG);
                    columnTypes.add(ColumnType.LONG);
                    columnTypes.add(ColumnType.LONG);
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, columnTypes);
                    final int initialBufferSize = configuration.getSqlWindowInitialRangeBufferSize();
                    MemoryARW mem;
                    try {
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                    } catch (Throwable t) {
                        Misc.free(map);
                        throw t;
                    }
                    try {
                        return new Decimal8NthValueOverPartitionRangeFrameFunction(map, partitionByRecord, partitionBySink,
                                rowsLo, rowsHi, args.get(0), mem, initialBufferSize, timestampIndex, n, argType);
                    } catch (Throwable t) {
                        Misc.free(map);
                        Misc.free(mem);
                        throw t;
                    }
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, NTH_VALUE_DECIMAL64_TYPES);
                    try {
                        return new Decimal8NthValueOverUnboundedPartitionFrameFunction(map, partitionByRecord, partitionBySink, args.get(0), n, false, argType);
                    } catch (Throwable t) {
                        Misc.free(map);
                        throw t;
                    }
                } else if (rowsLo == 0 && rowsHi == 0) {
                    return new Decimal8NthValueOverCurrentRowFunction(args.get(0), n, argType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, NTH_VALUE_DECIMAL64_TYPES);
                    try {
                        return new Decimal8NthValueOverPartitionFunction(map, partitionByRecord, partitionBySink, args.get(0), n, argType);
                    } catch (Throwable t) {
                        Misc.free(map);
                        throw t;
                    }
                } else if (rowsLo == Long.MIN_VALUE) {
                    ArrayColumnTypes columnTypes = new ArrayColumnTypes();
                    columnTypes.add(ColumnType.LONG);
                    columnTypes.add(ColumnType.LONG);
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, columnTypes);
                    try {
                        return new Decimal8NthValueOverPartitionRowsFrameUnboundedFunction(map, partitionByRecord, partitionBySink, rowsHi, args.get(0), n, argType);
                    } catch (Throwable t) {
                        Misc.free(map);
                        throw t;
                    }
                } else {
                    ArrayColumnTypes columnTypes = new ArrayColumnTypes();
                    columnTypes.add(ColumnType.LONG);
                    columnTypes.add(ColumnType.LONG);
                    columnTypes.add(ColumnType.LONG);
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, columnTypes);
                    MemoryARW mem;
                    try {
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                    } catch (Throwable t) {
                        Misc.free(map);
                        throw t;
                    }
                    try {
                        return new Decimal8NthValueOverPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink,
                                rowsLo, rowsHi, args.get(0), mem, n, argType);
                    } catch (Throwable t) {
                        Misc.free(map);
                        Misc.free(mem);
                        throw t;
                    }
                }
            }
        } else {
            if (framingMode == WindowExpression.FRAMING_RANGE) {
                if (!windowContext.isOrdered() && windowContext.isDefaultFrame()) {
                    return new Decimal8NthValueOverWholeResultSetFunction(args.get(0), n, argType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    return new Decimal8NthValueOverWholeResultSetFunction(args.get(0), n, argType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    return new Decimal8NthValueOverUnboundedRowsFrameFunction(args.get(0), n, argType);
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    return new Decimal8NthValueOverRangeFrameFunction(rowsLo, rowsHi, args.get(0), configuration, timestampIndex, n, argType);
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    return new Decimal8NthValueOverWholeResultSetFunction(args.get(0), n, argType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    return new Decimal8NthValueOverUnboundedRowsFrameFunction(args.get(0), n, argType);
                } else if (rowsLo == 0 && rowsHi == 0) {
                    return new Decimal8NthValueOverCurrentRowFunction(args.get(0), n, argType);
                } else if (rowsLo == Long.MIN_VALUE) {
                    return new Decimal8NthValueOverRowsFrameUnboundedFunction(args.get(0), rowsHi, n, argType);
                } else {
                    MemoryARW mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                            configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                    try {
                        return new Decimal8NthValueOverRowsFrameFunction(args.get(0), rowsLo, rowsHi, mem, n, argType);
                    } catch (Throwable t) {
                        Misc.free(mem);
                        throw t;
                    }
                }
            }
        }

        throw SqlException.$(position, "function not implemented for given window parameters");
    }

    private static ArrayColumnTypes NTH_VALUE_DECIMAL128_TYPES;
    private static ArrayColumnTypes NTH_VALUE_DECIMAL256_TYPES;

    static {
        NTH_VALUE_DECIMAL128_TYPES = new ArrayColumnTypes();
        NTH_VALUE_DECIMAL128_TYPES.add(ColumnType.DECIMAL128);
        NTH_VALUE_DECIMAL128_TYPES.add(ColumnType.LONG);

        NTH_VALUE_DECIMAL256_TYPES = new ArrayColumnTypes();
        NTH_VALUE_DECIMAL256_TYPES.add(ColumnType.DECIMAL256);
        NTH_VALUE_DECIMAL256_TYPES.add(ColumnType.LONG);
    }

    private Function newInstanceDecimal256(
            int position,
            ObjList<Function> args,
            CairoConfiguration configuration,
            WindowContext windowContext,
            int n,
            int argType
    ) throws SqlException {
        long rowsLo = windowContext.getRowsLo();
        long rowsHi = windowContext.getRowsHi();
        if (windowContext.getFramingMode() == WindowExpression.FRAMING_ROWS) {
            if (rowsLo != Long.MIN_VALUE && Math.abs(rowsLo) > Integer.MAX_VALUE) {
                throw SqlException.$(windowContext.getRowsLoKindPos(), "frame start exceeds maximum supported size");
            }
            if (rowsHi != Long.MAX_VALUE && Math.abs(rowsHi) > Integer.MAX_VALUE) {
                throw SqlException.$(windowContext.getRowsHiKindPos(), "frame end exceeds maximum supported size");
            }
        }

        int framingMode = windowContext.getFramingMode();
        RecordSink partitionBySink = windowContext.getPartitionBySink();
        ColumnTypes partitionByKeyTypes = windowContext.getPartitionByKeyTypes();
        VirtualRecord partitionByRecord = windowContext.getPartitionByRecord();

        if (partitionByRecord != null) {
            if (framingMode == WindowExpression.FRAMING_RANGE) {
                if (windowContext.isDefaultFrame() && (!windowContext.isOrdered() || windowContext.getRowsHi() == Long.MAX_VALUE)) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, NTH_VALUE_DECIMAL256_TYPES);
                    try {
                        return new Decimal256NthValueOverPartitionFunction(map, partitionByRecord, partitionBySink, args.get(0), n, argType);
                    } catch (Throwable t) {
                        Misc.free(map);
                        throw t;
                    }
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, NTH_VALUE_DECIMAL256_TYPES);
                    try {
                        return new Decimal256NthValueOverUnboundedPartitionFrameFunction(map, partitionByRecord, partitionBySink, args.get(0), n, true, argType);
                    } catch (Throwable t) {
                        Misc.free(map);
                        throw t;
                    }
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    ArrayColumnTypes columnTypes = new ArrayColumnTypes();
                    columnTypes.add(ColumnType.LONG);
                    columnTypes.add(ColumnType.LONG);
                    columnTypes.add(ColumnType.LONG);
                    columnTypes.add(ColumnType.LONG);
                    columnTypes.add(ColumnType.LONG);
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, columnTypes);
                    final int initialBufferSize = configuration.getSqlWindowInitialRangeBufferSize();
                    MemoryARW mem;
                    try {
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                    } catch (Throwable t) {
                        Misc.free(map);
                        throw t;
                    }
                    try {
                        return new Decimal256NthValueOverPartitionRangeFrameFunction(map, partitionByRecord, partitionBySink,
                                rowsLo, rowsHi, args.get(0), mem, initialBufferSize, timestampIndex, n, argType);
                    } catch (Throwable t) {
                        Misc.free(map);
                        Misc.free(mem);
                        throw t;
                    }
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, NTH_VALUE_DECIMAL256_TYPES);
                    try {
                        return new Decimal256NthValueOverUnboundedPartitionFrameFunction(map, partitionByRecord, partitionBySink, args.get(0), n, false, argType);
                    } catch (Throwable t) {
                        Misc.free(map);
                        throw t;
                    }
                } else if (rowsLo == 0 && rowsHi == 0) {
                    return new Decimal256NthValueOverCurrentRowFunction(args.get(0), n, argType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, NTH_VALUE_DECIMAL256_TYPES);
                    try {
                        return new Decimal256NthValueOverPartitionFunction(map, partitionByRecord, partitionBySink, args.get(0), n, argType);
                    } catch (Throwable t) {
                        Misc.free(map);
                        throw t;
                    }
                } else if (rowsLo == Long.MIN_VALUE) {
                    ArrayColumnTypes columnTypes = new ArrayColumnTypes();
                    columnTypes.add(ColumnType.LONG);
                    columnTypes.add(ColumnType.DECIMAL256);
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, columnTypes);
                    try {
                        return new Decimal256NthValueOverPartitionRowsFrameUnboundedFunction(map, partitionByRecord, partitionBySink, rowsHi, args.get(0), n, argType);
                    } catch (Throwable t) {
                        Misc.free(map);
                        throw t;
                    }
                } else {
                    ArrayColumnTypes columnTypes = new ArrayColumnTypes();
                    columnTypes.add(ColumnType.LONG);
                    columnTypes.add(ColumnType.LONG);
                    columnTypes.add(ColumnType.LONG);
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, columnTypes);
                    MemoryARW mem;
                    try {
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                    } catch (Throwable t) {
                        Misc.free(map);
                        throw t;
                    }
                    try {
                        return new Decimal256NthValueOverPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink,
                                rowsLo, rowsHi, args.get(0), mem, n, argType);
                    } catch (Throwable t) {
                        Misc.free(map);
                        Misc.free(mem);
                        throw t;
                    }
                }
            }
        } else {
            if (framingMode == WindowExpression.FRAMING_RANGE) {
                if (!windowContext.isOrdered() && windowContext.isDefaultFrame()) {
                    return new Decimal256NthValueOverWholeResultSetFunction(args.get(0), n, argType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    return new Decimal256NthValueOverWholeResultSetFunction(args.get(0), n, argType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    return new Decimal256NthValueOverUnboundedRowsFrameFunction(args.get(0), n, argType);
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    return new Decimal256NthValueOverRangeFrameFunction(rowsLo, rowsHi, args.get(0), configuration, timestampIndex, n, argType);
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    return new Decimal256NthValueOverWholeResultSetFunction(args.get(0), n, argType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    return new Decimal256NthValueOverUnboundedRowsFrameFunction(args.get(0), n, argType);
                } else if (rowsLo == 0 && rowsHi == 0) {
                    return new Decimal256NthValueOverCurrentRowFunction(args.get(0), n, argType);
                } else if (rowsLo == Long.MIN_VALUE) {
                    return new Decimal256NthValueOverRowsFrameUnboundedFunction(args.get(0), rowsHi, n, argType);
                } else {
                    MemoryARW mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                            configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                    try {
                        return new Decimal256NthValueOverRowsFrameFunction(args.get(0), rowsLo, rowsHi, mem, n, argType);
                    } catch (Throwable t) {
                        Misc.free(mem);
                        throw t;
                    }
                }
            }
        }

        throw SqlException.$(position, "function not implemented for given window parameters");
    }

    static class Decimal256NthValueOverCurrentRowFunction extends BaseWindowFunction implements WindowDecimal256Function {

        private final int n;
        private final int type;
        private final Decimal256 value = new Decimal256();

        Decimal256NthValueOverCurrentRowFunction(Function arg, int n, int type) {
            super(arg);
            this.n = n;
            this.type = type;
        }

        @Override
        public void computeNext(Record record) {
            if (n == 1) {
                arg.getDecimal256(record, value);
            } else {
                value.ofRawNull();
            }
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(value);
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

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(',').val(n).val(')');
            sink.val(" over (rows between current row and current row)");
        }
    }

    static class Decimal256NthValueOverPartitionFunction extends BasePartitionedWindowFunction implements WindowDecimal256Function {

        private final int n;
        private final Decimal256 scratch = new Decimal256();
        private final int type;

        public Decimal256NthValueOverPartitionFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, int n, int type) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.n = n;
            this.type = type;
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public int getPassCount() {
            return WindowFunction.TWO_PASS;
        }

        @Override
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.createValue();

            if (value.isNew()) {
                if (n == 1) {
                    arg.getDecimal256(record, scratch);
                    value.putDecimal256(0, scratch);
                } else {
                    scratch.ofRawNull();
                    value.putDecimal256(0, scratch);
                }
                value.putLong(4, 1);
            } else {
                long count = value.getLong(4) + 1;
                value.putLong(4, count);
                if (count == n) {
                    arg.getDecimal256(record, scratch);
                    value.putDecimal256(0, scratch);
                }
            }
        }

        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.findValue();
            assert value != null;
            value.getDecimal256(0, scratch);
            long addr = spi.getAddress(recordOffset, columnIndex);
            Unsafe.putLong(addr, scratch.getHh());
            Unsafe.putLong(addr + Long.BYTES, scratch.getHl());
            Unsafe.putLong(addr + 2 * Long.BYTES, scratch.getLh());
            Unsafe.putLong(addr + 3 * Long.BYTES, scratch.getLl());
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(',').val(n).val(')');
            sink.val(" over (");
            sink.val("partition by ");
            sink.val(partitionByRecord.getFunctions());
            sink.val(')');
        }
    }

    public static class Decimal256NthValueOverPartitionRangeFrameFunction extends BasePartitionedWindowFunction implements WindowDecimal256Function {

        protected static final int RECORD_SIZE = Long.BYTES + 32;
        protected final boolean frameIncludesCurrentValue;
        protected final boolean frameLoBounded;
        protected final LongList freeList = new LongList();
        protected final int initialBufferSize;
        protected final long maxDiff;
        protected final MemoryARW memory;
        protected final RingBufferDesc memoryDesc = new RingBufferDesc();
        protected final long minDiff;
        protected final int n;
        protected final Decimal256 nthValue = new Decimal256();
        protected final Decimal256 scratch = new Decimal256();
        protected final int timestampIndex;
        protected final int type;

        public Decimal256NthValueOverPartitionRangeFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rangeLo,
                long rangeHi,
                Function arg,
                MemoryARW memory,
                int initialBufferSize,
                int timestampIdx,
                int n,
                int type
        ) {
            super(map, partitionByRecord, partitionBySink, arg);
            frameLoBounded = rangeLo != Long.MIN_VALUE;
            maxDiff = frameLoBounded ? Math.abs(rangeLo) : Math.abs(rangeHi);
            minDiff = Math.abs(rangeHi);
            this.memory = memory;
            this.initialBufferSize = initialBufferSize;
            this.timestampIndex = timestampIdx;
            this.n = n;
            this.frameIncludesCurrentValue = rangeHi == 0;
            this.type = type;
        }

        @Override
        public void close() {
            super.close();
            memory.close();
            freeList.clear();
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mapValue = key.createValue();

            long frameSize;
            long startOffset;
            long size;
            long capacity;
            long firstIdx;
            long timestamp = record.getTimestamp(timestampIndex);
            arg.getDecimal256(record, scratch);

            if (mapValue.isNew()) {
                capacity = initialBufferSize;
                startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
                firstIdx = 0;
                memory.putLong(startOffset, timestamp);
                memory.putDecimal256(startOffset + Long.BYTES, scratch.getHh(), scratch.getHl(), scratch.getLh(), scratch.getLl());
                size = 1;
                if (frameIncludesCurrentValue) {
                    frameSize = 1;
                    if (n == 1) {
                        nthValue.copyRaw(scratch);
                    } else {
                        nthValue.ofRawNull();
                    }
                } else {
                    frameSize = 0;
                    nthValue.ofRawNull();
                }
            } else {
                frameSize = mapValue.getLong(0);
                startOffset = mapValue.getLong(1);
                size = mapValue.getLong(2);
                capacity = mapValue.getLong(3);
                firstIdx = mapValue.getLong(4);

                if (!frameLoBounded && frameSize >= n) {
                    long nthIdx = (firstIdx + n - 1) % capacity;
                    memory.getDecimal256(startOffset + nthIdx * RECORD_SIZE + Long.BYTES, nthValue);
                    return;
                }

                long newFirstIdx = firstIdx;
                if (frameLoBounded) {
                    for (long i = 0, nn = size; i < nn; i++) {
                        long idx = (firstIdx + i) % capacity;
                        long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                        if (Math.abs(timestamp - ts) > maxDiff) {
                            if (frameSize > 0) {
                                frameSize--;
                            }
                            newFirstIdx = (idx + 1) % capacity;
                            size--;
                        } else {
                            break;
                        }
                    }
                }
                firstIdx = newFirstIdx;

                if (size == capacity) {
                    memoryDesc.reset(capacity, startOffset, size, firstIdx, freeList);
                    expandRingBuffer(memory, memoryDesc, RECORD_SIZE);
                    capacity = memoryDesc.capacity;
                    startOffset = memoryDesc.startOffset;
                    firstIdx = memoryDesc.firstIdx;
                }
                memory.putLong(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE, timestamp);
                memory.putDecimal256(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE + Long.BYTES, scratch.getHh(), scratch.getHl(), scratch.getLh(), scratch.getLl());
                size++;

                if (frameLoBounded) {
                    for (long i = frameSize; i < size; i++) {
                        long idx = (firstIdx + i) % capacity;
                        long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                        long diff = Math.abs(ts - timestamp);
                        if (diff <= maxDiff && diff >= minDiff) {
                            frameSize++;
                        } else {
                            break;
                        }
                    }
                } else {
                    for (long i = frameSize; i < size; i++) {
                        long idx = (firstIdx + i) % capacity;
                        long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                        if (Math.abs(timestamp - ts) >= minDiff) {
                            frameSize++;
                        } else {
                            break;
                        }
                    }
                }

                if (frameSize >= n) {
                    long nthIdx = (firstIdx + n - 1) % capacity;
                    memory.getDecimal256(startOffset + nthIdx * RECORD_SIZE + Long.BYTES, nthValue);
                } else {
                    nthValue.ofRawNull();
                }
            }

            mapValue.putLong(0, frameSize);
            mapValue.putLong(1, startOffset);
            mapValue.putLong(2, size);
            mapValue.putLong(3, capacity);
            mapValue.putLong(4, firstIdx);
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(nthValue);
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
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            long addr = spi.getAddress(recordOffset, columnIndex);
            Unsafe.putLong(addr, nthValue.getHh());
            Unsafe.putLong(addr + Long.BYTES, nthValue.getHl());
            Unsafe.putLong(addr + 2 * Long.BYTES, nthValue.getLh());
            Unsafe.putLong(addr + 3 * Long.BYTES, nthValue.getLl());
        }

        @Override
        public void reopen() {
            super.reopen();
            freeList.clear();
            nthValue.ofRawNull();
        }

        @Override
        public void reset() {
            super.reset();
            memory.close();
            freeList.clear();
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(',').val(n).val(')');
            sink.val(" over (");
            sink.val("partition by ");
            sink.val(partitionByRecord.getFunctions());
            sink.val(" range between ");
            if (frameLoBounded) {
                sink.val(maxDiff).val(" preceding");
            } else {
                sink.val("unbounded preceding");
            }
            sink.val(" and ");
            if (minDiff == 0) {
                sink.val("current row");
            } else {
                sink.val(minDiff).val(" preceding");
            }
            sink.val(')');
        }

        @Override
        public void toTop() {
            super.toTop();
            memory.truncate();
            freeList.clear();
        }
    }

    public static class Decimal256NthValueOverPartitionRowsFrameFunction extends BasePartitionedWindowFunction implements WindowDecimal256Function {

        protected final int bufferSize;
        protected final int excludeCount;
        protected final boolean frameIncludesCurrentValue;
        protected final int frameSize;
        protected final MemoryARW memory;
        protected final int n;
        protected final Decimal256 nthValue = new Decimal256();
        protected final Decimal256 scratch = new Decimal256();
        protected final int type;

        public Decimal256NthValueOverPartitionRowsFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rowsLo,
                long rowsHi,
                Function arg,
                MemoryARW memory,
                int n,
                int type
        ) {
            super(map, partitionByRecord, partitionBySink, arg);
            assert rowsLo > Long.MIN_VALUE;
            frameSize = (int) (rowsHi - rowsLo + (rowsHi < 0 ? 1 : 0));
            bufferSize = (int) Math.abs(rowsLo);
            this.excludeCount = rowsHi < 0 ? (int) Math.abs(rowsHi) : 0;
            this.frameIncludesCurrentValue = rowsHi == 0;
            this.memory = memory;
            this.n = n;
            this.type = type;
        }

        @Override
        public void close() {
            super.close();
            memory.close();
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mapValue = key.createValue();

            arg.getDecimal256(record, scratch);

            long loIdx;
            long startOffset;
            long count;

            if (mapValue.isNew()) {
                loIdx = 0;
                count = 0;
                startOffset = memory.appendAddressFor((long) bufferSize * 32L) - memory.getPageAddress(0);
                mapValue.putLong(1, startOffset);
                Decimal256 nullScratch = new Decimal256();
                nullScratch.ofRawNull();
                for (int i = 0; i < bufferSize; i++) {
                    memory.putDecimal256(startOffset + (long) i * 32L, nullScratch.getHh(), nullScratch.getHl(), nullScratch.getLh(), nullScratch.getLl());
                }
            } else {
                loIdx = mapValue.getLong(0);
                startOffset = mapValue.getLong(1);
                count = mapValue.getLong(2);
            }

            long effectiveCount = Math.min(count, bufferSize);
            long currentFrameSize;
            if (frameIncludesCurrentValue) {
                currentFrameSize = Math.min(effectiveCount, frameSize);
            } else {
                currentFrameSize = Math.clamp(count + 1 - excludeCount, 0, frameSize);
            }

            if (currentFrameSize > 0 || (count == 0 && frameIncludesCurrentValue)) {
                long frameStartIdx = (loIdx + bufferSize - effectiveCount) % bufferSize;
                if (frameIncludesCurrentValue) {
                    if (n <= currentFrameSize) {
                        long nthIdx = (frameStartIdx + n - 1) % bufferSize;
                        memory.getDecimal256(startOffset + nthIdx * 32L, nthValue);
                    } else if (n == currentFrameSize + 1) {
                        nthValue.copyRaw(scratch);
                    } else {
                        nthValue.ofRawNull();
                    }
                } else {
                    if (n <= currentFrameSize) {
                        long nthIdx = (frameStartIdx + n - 1) % bufferSize;
                        memory.getDecimal256(startOffset + nthIdx * 32L, nthValue);
                    } else {
                        nthValue.ofRawNull();
                    }
                }
            } else {
                nthValue.ofRawNull();
            }

            count = Math.min(count + 1, bufferSize);
            mapValue.putLong(0, (loIdx + 1) % bufferSize);
            mapValue.putLong(2, count);
            memory.putDecimal256(startOffset + loIdx * 32L, scratch.getHh(), scratch.getHl(), scratch.getLh(), scratch.getLl());
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(nthValue);
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
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            long addr = spi.getAddress(recordOffset, columnIndex);
            Unsafe.putLong(addr, nthValue.getHh());
            Unsafe.putLong(addr + Long.BYTES, nthValue.getHl());
            Unsafe.putLong(addr + 2 * Long.BYTES, nthValue.getLh());
            Unsafe.putLong(addr + 3 * Long.BYTES, nthValue.getLl());
        }

        @Override
        public void reopen() {
            super.reopen();
        }

        @Override
        public void reset() {
            super.reset();
            memory.close();
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(',').val(n).val(')');
            sink.val(" over (");
            sink.val("partition by ");
            sink.val(partitionByRecord.getFunctions());
            sink.val(" rows between ");
            sink.val(bufferSize);
            sink.val(" preceding and ");
            if (frameIncludesCurrentValue) {
                sink.val("current row");
            } else {
                sink.val(excludeCount).val(" preceding");
            }
            sink.val(')');
        }

        @Override
        public void toTop() {
            super.toTop();
            memory.truncate();
        }
    }

    public static class Decimal256NthValueOverPartitionRowsFrameUnboundedFunction extends BasePartitionedWindowFunction implements WindowDecimal256Function {

        protected final int bufferSize;
        protected final int n;
        protected final Decimal256 nthValue = new Decimal256();
        protected final Decimal256 scratch = new Decimal256();
        protected final int type;

        public Decimal256NthValueOverPartitionRowsFrameUnboundedFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rowsHi,
                Function arg,
                int n,
                int type
        ) {
            super(map, partitionByRecord, partitionBySink, arg);
            assert rowsHi < 0 && rowsHi != Long.MIN_VALUE;
            this.bufferSize = (int) Math.abs(rowsHi);
            this.n = n;
            this.type = type;
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mapValue = key.createValue();

            arg.getDecimal256(record, scratch);
            long count;
            if (mapValue.isNew()) {
                count = 0;
                nthValue.ofRawNull();
                mapValue.putDecimal256(1, nthValue);
            } else {
                count = mapValue.getLong(0);
            }
            count++;
            if (count == n) {
                mapValue.putDecimal256(1, scratch);
            }
            if (count >= (long) n + bufferSize) {
                mapValue.getDecimal256(1, nthValue);
            } else {
                nthValue.ofRawNull();
            }
            mapValue.putLong(0, count);
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(nthValue);
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
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            long addr = spi.getAddress(recordOffset, columnIndex);
            Unsafe.putLong(addr, nthValue.getHh());
            Unsafe.putLong(addr + Long.BYTES, nthValue.getHl());
            Unsafe.putLong(addr + 2 * Long.BYTES, nthValue.getLh());
            Unsafe.putLong(addr + 3 * Long.BYTES, nthValue.getLl());
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(',').val(n).val(')');
            sink.val(" over (");
            sink.val("partition by ");
            sink.val(partitionByRecord.getFunctions());
            sink.val(" rows between unbounded preceding and ");
            sink.val(bufferSize).val(" preceding");
            sink.val(')');
        }
    }

    public static class Decimal256NthValueOverRangeFrameFunction extends BaseWindowFunction implements Reopenable, WindowDecimal256Function {
        protected static final int RECORD_SIZE = Long.BYTES + 32;
        protected final boolean frameIncludesCurrentValue;
        protected final boolean frameLoBounded;
        protected final LongList freeList = new LongList();
        protected final long initialCapacity;
        protected final long maxDiff;
        protected final MemoryARW memory;
        protected final RingBufferDesc memoryDesc = new RingBufferDesc();
        protected final long minDiff;
        protected final int n;
        protected final Decimal256 nthValue = new Decimal256();
        protected final Decimal256 scratch = new Decimal256();
        protected final int timestampIndex;
        protected final int type;
        protected long capacity;
        protected long firstIdx;
        protected long frameSize;
        protected long size;
        protected long startOffset;

        public Decimal256NthValueOverRangeFrameFunction(
                long rangeLo,
                long rangeHi,
                Function arg,
                CairoConfiguration configuration,
                int timestampIdx,
                int n,
                int type
        ) {
            super(arg);
            frameLoBounded = rangeLo != Long.MIN_VALUE;
            maxDiff = frameLoBounded ? Math.abs(rangeLo) : Math.abs(rangeHi);
            minDiff = Math.abs(rangeHi);
            timestampIndex = timestampIdx;
            initialCapacity = configuration.getSqlWindowStorePageSize() / RECORD_SIZE;
            capacity = initialCapacity;
            MemoryARW mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                    configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
            try {
                startOffset = mem.appendAddressFor(capacity * RECORD_SIZE) - mem.getPageAddress(0);
            } catch (Throwable t) {
                Misc.free(mem);
                throw t;
            }
            memory = mem;
            firstIdx = 0;
            frameSize = 0;
            frameIncludesCurrentValue = rangeHi == 0;
            this.n = n;
            this.type = type;
        }

        @Override
        public void close() {
            super.close();
            memory.close();
        }

        @Override
        public void computeNext(Record record) {
            if (!frameLoBounded && frameSize >= n) {
                long nthIdx = (firstIdx + n - 1) % capacity;
                memory.getDecimal256(startOffset + nthIdx * RECORD_SIZE + Long.BYTES, nthValue);
                return;
            }

            long timestamp = record.getTimestamp(timestampIndex);
            arg.getDecimal256(record, scratch);
            long newFirstIdx = firstIdx;

            if (frameLoBounded) {
                for (long i = 0, nn = size; i < nn; i++) {
                    long idx = (firstIdx + i) % capacity;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    if (Math.abs(timestamp - ts) > maxDiff) {
                        if (frameSize > 0) {
                            frameSize--;
                        }
                        newFirstIdx = (idx + 1) % capacity;
                        size--;
                    } else {
                        break;
                    }
                }
            }
            firstIdx = newFirstIdx;

            if (size == capacity) {
                memoryDesc.reset(capacity, startOffset, size, firstIdx, freeList);
                expandRingBuffer(memory, memoryDesc, RECORD_SIZE);
                capacity = memoryDesc.capacity;
                startOffset = memoryDesc.startOffset;
                firstIdx = memoryDesc.firstIdx;
            }
            memory.putLong(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE, timestamp);
            memory.putDecimal256(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE + Long.BYTES, scratch.getHh(), scratch.getHl(), scratch.getLh(), scratch.getLl());
            size++;

            if (frameLoBounded) {
                for (long i = frameSize, nn = size; i < nn; i++) {
                    long idx = (firstIdx + i) % capacity;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    long diff = Math.abs(ts - timestamp);
                    if (diff <= maxDiff && diff >= minDiff) {
                        frameSize++;
                    } else {
                        break;
                    }
                }
            } else {
                for (long i = frameSize, nn = size; i < nn; i++) {
                    long idx = (firstIdx + i) % capacity;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    if (Math.abs(timestamp - ts) >= minDiff) {
                        frameSize++;
                    } else {
                        break;
                    }
                }
            }

            if (frameSize >= n) {
                long nthIdx = (firstIdx + n - 1) % capacity;
                memory.getDecimal256(startOffset + nthIdx * RECORD_SIZE + Long.BYTES, nthValue);
            } else {
                nthValue.ofRawNull();
            }
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(nthValue);
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
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            long addr = spi.getAddress(recordOffset, columnIndex);
            Unsafe.putLong(addr, nthValue.getHh());
            Unsafe.putLong(addr + Long.BYTES, nthValue.getHl());
            Unsafe.putLong(addr + 2 * Long.BYTES, nthValue.getLh());
            Unsafe.putLong(addr + 3 * Long.BYTES, nthValue.getLl());
        }

        @Override
        public void reopen() {
            nthValue.ofRawNull();
            capacity = initialCapacity;
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            frameSize = 0;
            size = 0;
            freeList.clear();
        }

        @Override
        public void reset() {
            super.reset();
            memory.close();
            freeList.clear();
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(',').val(n).val(')');
            sink.val(" over (");
            sink.val("range between ");
            if (frameLoBounded) {
                sink.val(maxDiff).val(" preceding");
            } else {
                sink.val("unbounded preceding");
            }
            sink.val(" and ");
            if (minDiff == 0) {
                sink.val("current row");
            } else {
                sink.val(minDiff).val(" preceding");
            }
            sink.val(')');
        }

        @Override
        public void toTop() {
            super.toTop();
            nthValue.ofRawNull();
            capacity = initialCapacity;
            memory.truncate();
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            frameSize = 0;
            size = 0;
            freeList.clear();
        }
    }

    public static class Decimal256NthValueOverRowsFrameFunction extends BaseWindowFunction implements Reopenable, WindowDecimal256Function {
        protected final MemoryARW buffer;
        protected final int bufferSize;
        protected final int excludeCount;
        protected final boolean frameIncludesCurrentValue;
        protected final int frameSize;
        protected final int n;
        protected final Decimal256 nthValue = new Decimal256();
        protected final Decimal256 scratch = new Decimal256();
        protected final int type;
        protected long count = 0;
        protected int loIdx = 0;

        public Decimal256NthValueOverRowsFrameFunction(Function arg, long rowsLo, long rowsHi, MemoryARW memory, int n, int type) {
            super(arg);
            assert rowsLo > Long.MIN_VALUE;
            frameSize = (int) (rowsHi - rowsLo + (rowsHi < 0 ? 1 : 0));
            bufferSize = (int) Math.abs(rowsLo);
            excludeCount = rowsHi < 0 ? (int) Math.abs(rowsHi) : 0;
            frameIncludesCurrentValue = rowsHi == 0;
            this.buffer = memory;
            this.n = n;
            this.type = type;
            initBuffer();
        }

        @Override
        public void close() {
            super.close();
            buffer.close();
        }

        @Override
        public void computeNext(Record record) {
            arg.getDecimal256(record, scratch);

            long effectiveCount = Math.min(count, bufferSize);
            long currentFrameElements;
            if (frameIncludesCurrentValue) {
                currentFrameElements = Math.min(effectiveCount, frameSize);
            } else {
                currentFrameElements = Math.clamp(count + 1 - excludeCount, 0, frameSize);
            }

            if (currentFrameElements > 0 || (count == 0 && frameIncludesCurrentValue)) {
                long frameStartIdx = (loIdx + bufferSize - effectiveCount) % bufferSize;
                if (frameIncludesCurrentValue) {
                    if (n <= currentFrameElements) {
                        long nthIdx = (frameStartIdx + n - 1) % bufferSize;
                        buffer.getDecimal256(nthIdx * 32L, nthValue);
                    } else if (n == currentFrameElements + 1) {
                        nthValue.copyRaw(scratch);
                    } else {
                        nthValue.ofRawNull();
                    }
                } else {
                    if (n <= currentFrameElements) {
                        long nthIdx = (frameStartIdx + n - 1) % bufferSize;
                        buffer.getDecimal256(nthIdx * 32L, nthValue);
                    } else {
                        nthValue.ofRawNull();
                    }
                }
            } else {
                nthValue.ofRawNull();
            }

            count = Math.min(count + 1, bufferSize);
            buffer.putDecimal256((long) loIdx * 32L, scratch.getHh(), scratch.getHl(), scratch.getLh(), scratch.getLl());
            loIdx = (loIdx + 1) % bufferSize;
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(nthValue);
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
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            long addr = spi.getAddress(recordOffset, columnIndex);
            Unsafe.putLong(addr, nthValue.getHh());
            Unsafe.putLong(addr + Long.BYTES, nthValue.getHl());
            Unsafe.putLong(addr + 2 * Long.BYTES, nthValue.getLh());
            Unsafe.putLong(addr + 3 * Long.BYTES, nthValue.getLl());
        }

        @Override
        public void reopen() {
            nthValue.ofRawNull();
            loIdx = 0;
            initBuffer();
            count = 0;
        }

        @Override
        public void reset() {
            super.reset();
            buffer.close();
            nthValue.ofRawNull();
            loIdx = 0;
            count = 0;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(',').val(n).val(')');
            sink.val(" over (");
            sink.val(" rows between ");
            sink.val(bufferSize);
            sink.val(" preceding and ");
            if (frameIncludesCurrentValue) {
                sink.val("current row");
            } else {
                sink.val(excludeCount).val(" preceding");
            }
            sink.val(')');
        }

        @Override
        public void toTop() {
            super.toTop();
            nthValue.ofRawNull();
            loIdx = 0;
            initBuffer();
            count = 0;
        }

        private void initBuffer() {
            Decimal256 nullScratch = new Decimal256();
            nullScratch.ofRawNull();
            for (int i = 0; i < bufferSize; i++) {
                buffer.putDecimal256((long) i * 32L, nullScratch.getHh(), nullScratch.getHl(), nullScratch.getLh(), nullScratch.getLl());
            }
        }
    }

    public static class Decimal256NthValueOverRowsFrameUnboundedFunction extends BaseWindowFunction implements Reopenable, WindowDecimal256Function {
        protected final int bufferSize;
        protected final Decimal256 lockedValue = new Decimal256();
        protected final int n;
        protected final Decimal256 nthValue = new Decimal256();
        protected final Decimal256 scratch = new Decimal256();
        protected final int type;
        protected long totalCount = 0;

        public Decimal256NthValueOverRowsFrameUnboundedFunction(Function arg, long rowsHi, int n, int type) {
            super(arg);
            assert rowsHi < 0 && rowsHi != Long.MIN_VALUE;
            this.bufferSize = (int) Math.abs(rowsHi);
            this.n = n;
            this.type = type;
            lockedValue.ofRawNull();
            nthValue.ofRawNull();
        }

        @Override
        public void computeNext(Record record) {
            arg.getDecimal256(record, scratch);
            totalCount++;
            if (totalCount == n) {
                lockedValue.copyRaw(scratch);
            }
            if (totalCount >= (long) n + bufferSize) {
                nthValue.copyRaw(lockedValue);
            } else {
                nthValue.ofRawNull();
            }
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(nthValue);
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
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            long addr = spi.getAddress(recordOffset, columnIndex);
            Unsafe.putLong(addr, nthValue.getHh());
            Unsafe.putLong(addr + Long.BYTES, nthValue.getHl());
            Unsafe.putLong(addr + 2 * Long.BYTES, nthValue.getLh());
            Unsafe.putLong(addr + 3 * Long.BYTES, nthValue.getLl());
        }

        @Override
        public void reopen() {
            nthValue.ofRawNull();
            lockedValue.ofRawNull();
            totalCount = 0;
        }

        @Override
        public void reset() {
            super.reset();
            nthValue.ofRawNull();
            lockedValue.ofRawNull();
            totalCount = 0;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(',').val(n).val(')');
            sink.val(" over (");
            sink.val(" rows between unbounded preceding and ");
            sink.val(bufferSize).val(" preceding");
            sink.val(')');
        }

        @Override
        public void toTop() {
            super.toTop();
            nthValue.ofRawNull();
            lockedValue.ofRawNull();
            totalCount = 0;
        }
    }

    static class Decimal256NthValueOverUnboundedPartitionFrameFunction extends BasePartitionedWindowFunction implements WindowDecimal256Function {

        protected final boolean isRange;
        protected final int n;
        protected final Decimal256 scratch = new Decimal256();
        protected final int type;
        protected final Decimal256 value = new Decimal256();

        public Decimal256NthValueOverUnboundedPartitionFrameFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, int n, boolean isRange, int type) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.n = n;
            this.isRange = isRange;
            this.type = type;
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mapValue = key.createValue();

            if (mapValue.isNew()) {
                if (n == 1) {
                    arg.getDecimal256(record, scratch);
                    mapValue.putDecimal256(0, scratch);
                    value.copyRaw(scratch);
                } else {
                    scratch.ofRawNull();
                    mapValue.putDecimal256(0, scratch);
                    value.ofRawNull();
                }
                mapValue.putLong(4, 1);
            } else {
                long count = mapValue.getLong(4);
                if (count >= n) {
                    mapValue.getDecimal256(0, value);
                } else {
                    count++;
                    mapValue.putLong(4, count);
                    if (count == n) {
                        arg.getDecimal256(record, scratch);
                        mapValue.putDecimal256(0, scratch);
                        value.copyRaw(scratch);
                    } else {
                        value.ofRawNull();
                    }
                }
            }
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(value);
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

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(',').val(n).val(')');
            sink.val(" over (");
            sink.val("partition by ");
            sink.val(partitionByRecord.getFunctions());
            sink.val(isRange ? " range" : " rows");
            sink.val(" between unbounded preceding and current row)");
        }
    }

    public static class Decimal256NthValueOverUnboundedRowsFrameFunction extends BaseWindowFunction implements WindowDecimal256Function {
        protected final int n;
        protected final int type;
        protected final Decimal256 value = new Decimal256();
        protected long count;
        protected boolean isFound;

        public Decimal256NthValueOverUnboundedRowsFrameFunction(Function arg, int n, int type) {
            super(arg);
            this.n = n;
            this.type = type;
            value.ofRawNull();
        }

        @Override
        public void computeNext(Record record) {
            if (!isFound) {
                count++;
                if (count == n) {
                    arg.getDecimal256(record, value);
                    isFound = true;
                }
            }
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(value);
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

        @Override
        public void reset() {
            super.reset();
            count = 0;
            isFound = false;
            value.ofRawNull();
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(',').val(n).val(')');
            sink.val(" over (rows between unbounded preceding and current row)");
        }

        @Override
        public void toTop() {
            super.toTop();
            count = 0;
            isFound = false;
            value.ofRawNull();
        }
    }

    public static class Decimal256NthValueOverWholeResultSetFunction extends BaseWindowFunction implements WindowDecimal256Function {
        protected final int n;
        protected final int type;
        protected final Decimal256 value = new Decimal256();
        protected long count;
        protected boolean isFound;

        public Decimal256NthValueOverWholeResultSetFunction(Function arg, int n, int type) {
            super(arg);
            this.n = n;
            this.type = type;
            value.ofRawNull();
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(value);
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public int getPassCount() {
            return WindowFunction.TWO_PASS;
        }

        @Override
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            if (!isFound) {
                count++;
                if (count == n) {
                    arg.getDecimal256(record, value);
                    isFound = true;
                }
            }
        }

        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            long addr = spi.getAddress(recordOffset, columnIndex);
            Unsafe.putLong(addr, value.getHh());
            Unsafe.putLong(addr + Long.BYTES, value.getHl());
            Unsafe.putLong(addr + 2 * Long.BYTES, value.getLh());
            Unsafe.putLong(addr + 3 * Long.BYTES, value.getLl());
        }

        @Override
        public void reset() {
            super.reset();
            clearState();
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(',').val(n).val(')');
            sink.val(" over ()");
        }

        @Override
        public void toTop() {
            super.toTop();
            clearState();
        }

        private void clearState() {
            count = 0;
            isFound = false;
            value.ofRawNull();
        }
    }

    private Function newInstanceDecimal128(
            int position,
            ObjList<Function> args,
            CairoConfiguration configuration,
            WindowContext windowContext,
            int n,
            int argType
    ) throws SqlException {
        long rowsLo = windowContext.getRowsLo();
        long rowsHi = windowContext.getRowsHi();
        if (windowContext.getFramingMode() == WindowExpression.FRAMING_ROWS) {
            if (rowsLo != Long.MIN_VALUE && Math.abs(rowsLo) > Integer.MAX_VALUE) {
                throw SqlException.$(windowContext.getRowsLoKindPos(), "frame start exceeds maximum supported size");
            }
            if (rowsHi != Long.MAX_VALUE && Math.abs(rowsHi) > Integer.MAX_VALUE) {
                throw SqlException.$(windowContext.getRowsHiKindPos(), "frame end exceeds maximum supported size");
            }
        }

        int framingMode = windowContext.getFramingMode();
        RecordSink partitionBySink = windowContext.getPartitionBySink();
        ColumnTypes partitionByKeyTypes = windowContext.getPartitionByKeyTypes();
        VirtualRecord partitionByRecord = windowContext.getPartitionByRecord();

        if (partitionByRecord != null) {
            if (framingMode == WindowExpression.FRAMING_RANGE) {
                if (windowContext.isDefaultFrame() && (!windowContext.isOrdered() || windowContext.getRowsHi() == Long.MAX_VALUE)) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, NTH_VALUE_DECIMAL128_TYPES);
                    try {
                        return new Decimal128NthValueOverPartitionFunction(map, partitionByRecord, partitionBySink, args.get(0), n, argType);
                    } catch (Throwable t) {
                        Misc.free(map);
                        throw t;
                    }
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, NTH_VALUE_DECIMAL128_TYPES);
                    try {
                        return new Decimal128NthValueOverUnboundedPartitionFrameFunction(map, partitionByRecord, partitionBySink, args.get(0), n, true, argType);
                    } catch (Throwable t) {
                        Misc.free(map);
                        throw t;
                    }
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    ArrayColumnTypes columnTypes = new ArrayColumnTypes();
                    columnTypes.add(ColumnType.LONG);
                    columnTypes.add(ColumnType.LONG);
                    columnTypes.add(ColumnType.LONG);
                    columnTypes.add(ColumnType.LONG);
                    columnTypes.add(ColumnType.LONG);
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, columnTypes);
                    final int initialBufferSize = configuration.getSqlWindowInitialRangeBufferSize();
                    MemoryARW mem;
                    try {
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                    } catch (Throwable t) {
                        Misc.free(map);
                        throw t;
                    }
                    try {
                        return new Decimal128NthValueOverPartitionRangeFrameFunction(map, partitionByRecord, partitionBySink,
                                rowsLo, rowsHi, args.get(0), mem, initialBufferSize, timestampIndex, n, argType);
                    } catch (Throwable t) {
                        Misc.free(map);
                        Misc.free(mem);
                        throw t;
                    }
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, NTH_VALUE_DECIMAL128_TYPES);
                    try {
                        return new Decimal128NthValueOverUnboundedPartitionFrameFunction(map, partitionByRecord, partitionBySink, args.get(0), n, false, argType);
                    } catch (Throwable t) {
                        Misc.free(map);
                        throw t;
                    }
                } else if (rowsLo == 0 && rowsHi == 0) {
                    return new Decimal128NthValueOverCurrentRowFunction(args.get(0), n, argType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, NTH_VALUE_DECIMAL128_TYPES);
                    try {
                        return new Decimal128NthValueOverPartitionFunction(map, partitionByRecord, partitionBySink, args.get(0), n, argType);
                    } catch (Throwable t) {
                        Misc.free(map);
                        throw t;
                    }
                } else if (rowsLo == Long.MIN_VALUE) {
                    ArrayColumnTypes columnTypes = new ArrayColumnTypes();
                    columnTypes.add(ColumnType.LONG);
                    columnTypes.add(ColumnType.DECIMAL128);
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, columnTypes);
                    try {
                        return new Decimal128NthValueOverPartitionRowsFrameUnboundedFunction(map, partitionByRecord, partitionBySink, rowsHi, args.get(0), n, argType);
                    } catch (Throwable t) {
                        Misc.free(map);
                        throw t;
                    }
                } else {
                    ArrayColumnTypes columnTypes = new ArrayColumnTypes();
                    columnTypes.add(ColumnType.LONG);
                    columnTypes.add(ColumnType.LONG);
                    columnTypes.add(ColumnType.LONG);
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, columnTypes);
                    MemoryARW mem;
                    try {
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                    } catch (Throwable t) {
                        Misc.free(map);
                        throw t;
                    }
                    try {
                        return new Decimal128NthValueOverPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink,
                                rowsLo, rowsHi, args.get(0), mem, n, argType);
                    } catch (Throwable t) {
                        Misc.free(map);
                        Misc.free(mem);
                        throw t;
                    }
                }
            }
        } else {
            if (framingMode == WindowExpression.FRAMING_RANGE) {
                if (!windowContext.isOrdered() && windowContext.isDefaultFrame()) {
                    return new Decimal128NthValueOverWholeResultSetFunction(args.get(0), n, argType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    return new Decimal128NthValueOverWholeResultSetFunction(args.get(0), n, argType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    return new Decimal128NthValueOverUnboundedRowsFrameFunction(args.get(0), n, argType);
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    return new Decimal128NthValueOverRangeFrameFunction(rowsLo, rowsHi, args.get(0), configuration, timestampIndex, n, argType);
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    return new Decimal128NthValueOverWholeResultSetFunction(args.get(0), n, argType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    return new Decimal128NthValueOverUnboundedRowsFrameFunction(args.get(0), n, argType);
                } else if (rowsLo == 0 && rowsHi == 0) {
                    return new Decimal128NthValueOverCurrentRowFunction(args.get(0), n, argType);
                } else if (rowsLo == Long.MIN_VALUE) {
                    return new Decimal128NthValueOverRowsFrameUnboundedFunction(args.get(0), rowsHi, n, argType);
                } else {
                    MemoryARW mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                            configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                    try {
                        return new Decimal128NthValueOverRowsFrameFunction(args.get(0), rowsLo, rowsHi, mem, n, argType);
                    } catch (Throwable t) {
                        Misc.free(mem);
                        throw t;
                    }
                }
            }
        }

        throw SqlException.$(position, "function not implemented for given window parameters");
    }

    static class Decimal128NthValueOverCurrentRowFunction extends BaseWindowFunction implements WindowDecimal128Function {

        private final int n;
        private final Decimal128 scratch = new Decimal128();
        private final int type;
        private final Decimal128 value = new Decimal128();

        Decimal128NthValueOverCurrentRowFunction(Function arg, int n, int type) {
            super(arg);
            this.n = n;
            this.type = type;
        }

        @Override
        public void computeNext(Record record) {
            if (n == 1) {
                arg.getDecimal128(record, value);
            } else {
                value.ofRawNull();
            }
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            sink.copyFrom(value);
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

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(',').val(n).val(')');
            sink.val(" over (rows between current row and current row)");
        }
    }

    static class Decimal128NthValueOverPartitionFunction extends BasePartitionedWindowFunction implements WindowDecimal128Function {

        private final int n;
        private final Decimal128 scratch = new Decimal128();
        private final int type;

        public Decimal128NthValueOverPartitionFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, int n, int type) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.n = n;
            this.type = type;
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public int getPassCount() {
            return WindowFunction.TWO_PASS;
        }

        @Override
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.createValue();

            if (value.isNew()) {
                if (n == 1) {
                    arg.getDecimal128(record, scratch);
                    value.putDecimal128(0, scratch);
                } else {
                    scratch.ofRawNull();
                    value.putDecimal128(0, scratch);
                }
                value.putLong(2, 1);
            } else {
                long count = value.getLong(2) + 1;
                value.putLong(2, count);
                if (count == n) {
                    arg.getDecimal128(record, scratch);
                    value.putDecimal128(0, scratch);
                }
            }
        }

        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.findValue();
            assert value != null;
            value.getDecimal128(0, scratch);
            long addr = spi.getAddress(recordOffset, columnIndex);
            Unsafe.putLong(addr, scratch.getHigh());
            Unsafe.putLong(addr + Long.BYTES, scratch.getLow());
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(',').val(n).val(')');
            sink.val(" over (");
            sink.val("partition by ");
            sink.val(partitionByRecord.getFunctions());
            sink.val(')');
        }
    }

    public static class Decimal128NthValueOverPartitionRangeFrameFunction extends BasePartitionedWindowFunction implements WindowDecimal128Function {

        protected static final int RECORD_SIZE = Long.BYTES + 16;
        protected final boolean frameIncludesCurrentValue;
        protected final boolean frameLoBounded;
        protected final LongList freeList = new LongList();
        protected final int initialBufferSize;
        protected final long maxDiff;
        protected final MemoryARW memory;
        protected final RingBufferDesc memoryDesc = new RingBufferDesc();
        protected final long minDiff;
        protected final int n;
        protected final Decimal128 nthValue = new Decimal128();
        protected final Decimal128 scratch = new Decimal128();
        protected final int timestampIndex;
        protected final int type;

        public Decimal128NthValueOverPartitionRangeFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rangeLo,
                long rangeHi,
                Function arg,
                MemoryARW memory,
                int initialBufferSize,
                int timestampIdx,
                int n,
                int type
        ) {
            super(map, partitionByRecord, partitionBySink, arg);
            frameLoBounded = rangeLo != Long.MIN_VALUE;
            maxDiff = frameLoBounded ? Math.abs(rangeLo) : Math.abs(rangeHi);
            minDiff = Math.abs(rangeHi);
            this.memory = memory;
            this.initialBufferSize = initialBufferSize;
            this.timestampIndex = timestampIdx;
            this.n = n;
            this.frameIncludesCurrentValue = rangeHi == 0;
            this.type = type;
        }

        @Override
        public void close() {
            super.close();
            memory.close();
            freeList.clear();
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mapValue = key.createValue();

            long frameSize;
            long startOffset;
            long size;
            long capacity;
            long firstIdx;
            long timestamp = record.getTimestamp(timestampIndex);
            arg.getDecimal128(record, scratch);

            if (mapValue.isNew()) {
                capacity = initialBufferSize;
                startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
                firstIdx = 0;
                memory.putLong(startOffset, timestamp);
                memory.putDecimal128(startOffset + Long.BYTES, scratch.getHigh(), scratch.getLow());
                size = 1;
                if (frameIncludesCurrentValue) {
                    frameSize = 1;
                    if (n == 1) {
                        nthValue.copyFrom(scratch);
                    } else {
                        nthValue.ofRawNull();
                    }
                } else {
                    frameSize = 0;
                    nthValue.ofRawNull();
                }
            } else {
                frameSize = mapValue.getLong(0);
                startOffset = mapValue.getLong(1);
                size = mapValue.getLong(2);
                capacity = mapValue.getLong(3);
                firstIdx = mapValue.getLong(4);

                if (!frameLoBounded && frameSize >= n) {
                    long nthIdx = (firstIdx + n - 1) % capacity;
                    memory.getDecimal128(startOffset + nthIdx * RECORD_SIZE + Long.BYTES, nthValue);
                    return;
                }

                long newFirstIdx = firstIdx;
                if (frameLoBounded) {
                    for (long i = 0, nn = size; i < nn; i++) {
                        long idx = (firstIdx + i) % capacity;
                        long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                        if (Math.abs(timestamp - ts) > maxDiff) {
                            if (frameSize > 0) {
                                frameSize--;
                            }
                            newFirstIdx = (idx + 1) % capacity;
                            size--;
                        } else {
                            break;
                        }
                    }
                }
                firstIdx = newFirstIdx;

                if (size == capacity) {
                    memoryDesc.reset(capacity, startOffset, size, firstIdx, freeList);
                    expandRingBuffer(memory, memoryDesc, RECORD_SIZE);
                    capacity = memoryDesc.capacity;
                    startOffset = memoryDesc.startOffset;
                    firstIdx = memoryDesc.firstIdx;
                }
                memory.putLong(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE, timestamp);
                memory.putDecimal128(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE + Long.BYTES, scratch.getHigh(), scratch.getLow());
                size++;

                if (frameLoBounded) {
                    for (long i = frameSize; i < size; i++) {
                        long idx = (firstIdx + i) % capacity;
                        long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                        long diff = Math.abs(ts - timestamp);
                        if (diff <= maxDiff && diff >= minDiff) {
                            frameSize++;
                        } else {
                            break;
                        }
                    }
                } else {
                    for (long i = frameSize; i < size; i++) {
                        long idx = (firstIdx + i) % capacity;
                        long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                        if (Math.abs(timestamp - ts) >= minDiff) {
                            frameSize++;
                        } else {
                            break;
                        }
                    }
                }

                if (frameSize >= n) {
                    long nthIdx = (firstIdx + n - 1) % capacity;
                    memory.getDecimal128(startOffset + nthIdx * RECORD_SIZE + Long.BYTES, nthValue);
                } else {
                    nthValue.ofRawNull();
                }
            }

            mapValue.putLong(0, frameSize);
            mapValue.putLong(1, startOffset);
            mapValue.putLong(2, size);
            mapValue.putLong(3, capacity);
            mapValue.putLong(4, firstIdx);
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            sink.copyFrom(nthValue);
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
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            long addr = spi.getAddress(recordOffset, columnIndex);
            Unsafe.putLong(addr, nthValue.getHigh());
            Unsafe.putLong(addr + Long.BYTES, nthValue.getLow());
        }

        @Override
        public void reopen() {
            super.reopen();
            freeList.clear();
            nthValue.ofRawNull();
        }

        @Override
        public void reset() {
            super.reset();
            memory.close();
            freeList.clear();
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(',').val(n).val(')');
            sink.val(" over (");
            sink.val("partition by ");
            sink.val(partitionByRecord.getFunctions());
            sink.val(" range between ");
            if (frameLoBounded) {
                sink.val(maxDiff).val(" preceding");
            } else {
                sink.val("unbounded preceding");
            }
            sink.val(" and ");
            if (minDiff == 0) {
                sink.val("current row");
            } else {
                sink.val(minDiff).val(" preceding");
            }
            sink.val(')');
        }

        @Override
        public void toTop() {
            super.toTop();
            memory.truncate();
            freeList.clear();
        }
    }

    public static class Decimal128NthValueOverPartitionRowsFrameFunction extends BasePartitionedWindowFunction implements WindowDecimal128Function {

        protected final int bufferSize;
        protected final int excludeCount;
        protected final boolean frameIncludesCurrentValue;
        protected final int frameSize;
        protected final MemoryARW memory;
        protected final int n;
        protected final Decimal128 nthValue = new Decimal128();
        protected final Decimal128 scratch = new Decimal128();
        protected final int type;

        public Decimal128NthValueOverPartitionRowsFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rowsLo,
                long rowsHi,
                Function arg,
                MemoryARW memory,
                int n,
                int type
        ) {
            super(map, partitionByRecord, partitionBySink, arg);
            assert rowsLo > Long.MIN_VALUE;
            frameSize = (int) (rowsHi - rowsLo + (rowsHi < 0 ? 1 : 0));
            bufferSize = (int) Math.abs(rowsLo);
            this.excludeCount = rowsHi < 0 ? (int) Math.abs(rowsHi) : 0;
            this.frameIncludesCurrentValue = rowsHi == 0;
            this.memory = memory;
            this.n = n;
            this.type = type;
        }

        @Override
        public void close() {
            super.close();
            memory.close();
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mapValue = key.createValue();

            arg.getDecimal128(record, scratch);

            long loIdx;
            long startOffset;
            long count;

            if (mapValue.isNew()) {
                loIdx = 0;
                count = 0;
                startOffset = memory.appendAddressFor((long) bufferSize * 16L) - memory.getPageAddress(0);
                mapValue.putLong(1, startOffset);
                for (int i = 0; i < bufferSize; i++) {
                    memory.putLong(startOffset + (long) i * 16L, Decimals.DECIMAL128_HI_NULL);
                    memory.putLong(startOffset + (long) i * 16L + Long.BYTES, Decimals.DECIMAL128_LO_NULL);
                }
            } else {
                loIdx = mapValue.getLong(0);
                startOffset = mapValue.getLong(1);
                count = mapValue.getLong(2);
            }

            long effectiveCount = Math.min(count, bufferSize);
            long currentFrameSize;
            if (frameIncludesCurrentValue) {
                currentFrameSize = Math.min(effectiveCount, frameSize);
            } else {
                currentFrameSize = Math.clamp(count + 1 - excludeCount, 0, frameSize);
            }

            if (currentFrameSize > 0 || (count == 0 && frameIncludesCurrentValue)) {
                long frameStartIdx = (loIdx + bufferSize - effectiveCount) % bufferSize;
                if (frameIncludesCurrentValue) {
                    if (n <= currentFrameSize) {
                        long nthIdx = (frameStartIdx + n - 1) % bufferSize;
                        memory.getDecimal128(startOffset + nthIdx * 16L, nthValue);
                    } else if (n == currentFrameSize + 1) {
                        nthValue.copyFrom(scratch);
                    } else {
                        nthValue.ofRawNull();
                    }
                } else {
                    if (n <= currentFrameSize) {
                        long nthIdx = (frameStartIdx + n - 1) % bufferSize;
                        memory.getDecimal128(startOffset + nthIdx * 16L, nthValue);
                    } else {
                        nthValue.ofRawNull();
                    }
                }
            } else {
                nthValue.ofRawNull();
            }

            count = Math.min(count + 1, bufferSize);
            mapValue.putLong(0, (loIdx + 1) % bufferSize);
            mapValue.putLong(2, count);
            memory.putDecimal128(startOffset + loIdx * 16L, scratch.getHigh(), scratch.getLow());
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            sink.copyFrom(nthValue);
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
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            long addr = spi.getAddress(recordOffset, columnIndex);
            Unsafe.putLong(addr, nthValue.getHigh());
            Unsafe.putLong(addr + Long.BYTES, nthValue.getLow());
        }

        @Override
        public void reopen() {
            super.reopen();
        }

        @Override
        public void reset() {
            super.reset();
            memory.close();
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(',').val(n).val(')');
            sink.val(" over (");
            sink.val("partition by ");
            sink.val(partitionByRecord.getFunctions());
            sink.val(" rows between ");
            sink.val(bufferSize);
            sink.val(" preceding and ");
            if (frameIncludesCurrentValue) {
                sink.val("current row");
            } else {
                sink.val(excludeCount).val(" preceding");
            }
            sink.val(')');
        }

        @Override
        public void toTop() {
            super.toTop();
            memory.truncate();
        }
    }

    public static class Decimal128NthValueOverPartitionRowsFrameUnboundedFunction extends BasePartitionedWindowFunction implements WindowDecimal128Function {

        protected final int bufferSize;
        protected final int n;
        protected final Decimal128 nthValue = new Decimal128();
        protected final Decimal128 scratch = new Decimal128();
        protected final int type;

        public Decimal128NthValueOverPartitionRowsFrameUnboundedFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rowsHi,
                Function arg,
                int n,
                int type
        ) {
            super(map, partitionByRecord, partitionBySink, arg);
            assert rowsHi < 0 && rowsHi != Long.MIN_VALUE;
            this.bufferSize = (int) Math.abs(rowsHi);
            this.n = n;
            this.type = type;
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mapValue = key.createValue();

            arg.getDecimal128(record, scratch);
            long count;
            if (mapValue.isNew()) {
                count = 0;
                nthValue.ofRawNull();
                mapValue.putDecimal128(1, nthValue);
            } else {
                count = mapValue.getLong(0);
            }
            count++;
            if (count == n) {
                mapValue.putDecimal128(1, scratch);
            }
            if (count >= (long) n + bufferSize) {
                mapValue.getDecimal128(1, nthValue);
            } else {
                nthValue.ofRawNull();
            }
            mapValue.putLong(0, count);
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            sink.copyFrom(nthValue);
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
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            long addr = spi.getAddress(recordOffset, columnIndex);
            Unsafe.putLong(addr, nthValue.getHigh());
            Unsafe.putLong(addr + Long.BYTES, nthValue.getLow());
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(',').val(n).val(')');
            sink.val(" over (");
            sink.val("partition by ");
            sink.val(partitionByRecord.getFunctions());
            sink.val(" rows between unbounded preceding and ");
            sink.val(bufferSize).val(" preceding");
            sink.val(')');
        }
    }

    public static class Decimal128NthValueOverRangeFrameFunction extends BaseWindowFunction implements Reopenable, WindowDecimal128Function {
        protected static final int RECORD_SIZE = Long.BYTES + 16;
        protected final boolean frameIncludesCurrentValue;
        protected final boolean frameLoBounded;
        protected final LongList freeList = new LongList();
        protected final long initialCapacity;
        protected final long maxDiff;
        protected final MemoryARW memory;
        protected final RingBufferDesc memoryDesc = new RingBufferDesc();
        protected final long minDiff;
        protected final int n;
        protected final Decimal128 nthValue = new Decimal128();
        protected final Decimal128 scratch = new Decimal128();
        protected final int timestampIndex;
        protected final int type;
        protected long capacity;
        protected long firstIdx;
        protected long frameSize;
        protected long size;
        protected long startOffset;

        public Decimal128NthValueOverRangeFrameFunction(
                long rangeLo,
                long rangeHi,
                Function arg,
                CairoConfiguration configuration,
                int timestampIdx,
                int n,
                int type
        ) {
            super(arg);
            frameLoBounded = rangeLo != Long.MIN_VALUE;
            maxDiff = frameLoBounded ? Math.abs(rangeLo) : Math.abs(rangeHi);
            minDiff = Math.abs(rangeHi);
            timestampIndex = timestampIdx;
            initialCapacity = configuration.getSqlWindowStorePageSize() / RECORD_SIZE;
            capacity = initialCapacity;
            MemoryARW mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                    configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
            try {
                startOffset = mem.appendAddressFor(capacity * RECORD_SIZE) - mem.getPageAddress(0);
            } catch (Throwable t) {
                Misc.free(mem);
                throw t;
            }
            memory = mem;
            firstIdx = 0;
            frameSize = 0;
            frameIncludesCurrentValue = rangeHi == 0;
            this.n = n;
            this.type = type;
        }

        @Override
        public void close() {
            super.close();
            memory.close();
        }

        @Override
        public void computeNext(Record record) {
            if (!frameLoBounded && frameSize >= n) {
                long nthIdx = (firstIdx + n - 1) % capacity;
                memory.getDecimal128(startOffset + nthIdx * RECORD_SIZE + Long.BYTES, nthValue);
                return;
            }

            long timestamp = record.getTimestamp(timestampIndex);
            arg.getDecimal128(record, scratch);
            long newFirstIdx = firstIdx;

            if (frameLoBounded) {
                for (long i = 0, nn = size; i < nn; i++) {
                    long idx = (firstIdx + i) % capacity;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    if (Math.abs(timestamp - ts) > maxDiff) {
                        if (frameSize > 0) {
                            frameSize--;
                        }
                        newFirstIdx = (idx + 1) % capacity;
                        size--;
                    } else {
                        break;
                    }
                }
            }
            firstIdx = newFirstIdx;

            if (size == capacity) {
                memoryDesc.reset(capacity, startOffset, size, firstIdx, freeList);
                expandRingBuffer(memory, memoryDesc, RECORD_SIZE);
                capacity = memoryDesc.capacity;
                startOffset = memoryDesc.startOffset;
                firstIdx = memoryDesc.firstIdx;
            }
            memory.putLong(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE, timestamp);
            memory.putDecimal128(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE + Long.BYTES, scratch.getHigh(), scratch.getLow());
            size++;

            if (frameLoBounded) {
                for (long i = frameSize, nn = size; i < nn; i++) {
                    long idx = (firstIdx + i) % capacity;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    long diff = Math.abs(ts - timestamp);
                    if (diff <= maxDiff && diff >= minDiff) {
                        frameSize++;
                    } else {
                        break;
                    }
                }
            } else {
                for (long i = frameSize, nn = size; i < nn; i++) {
                    long idx = (firstIdx + i) % capacity;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    if (Math.abs(timestamp - ts) >= minDiff) {
                        frameSize++;
                    } else {
                        break;
                    }
                }
            }

            if (frameSize >= n) {
                long nthIdx = (firstIdx + n - 1) % capacity;
                memory.getDecimal128(startOffset + nthIdx * RECORD_SIZE + Long.BYTES, nthValue);
            } else {
                nthValue.ofRawNull();
            }
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            sink.copyFrom(nthValue);
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
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            long addr = spi.getAddress(recordOffset, columnIndex);
            Unsafe.putLong(addr, nthValue.getHigh());
            Unsafe.putLong(addr + Long.BYTES, nthValue.getLow());
        }

        @Override
        public void reopen() {
            nthValue.ofRawNull();
            capacity = initialCapacity;
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            frameSize = 0;
            size = 0;
            freeList.clear();
        }

        @Override
        public void reset() {
            super.reset();
            memory.close();
            freeList.clear();
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(',').val(n).val(')');
            sink.val(" over (");
            sink.val("range between ");
            if (frameLoBounded) {
                sink.val(maxDiff).val(" preceding");
            } else {
                sink.val("unbounded preceding");
            }
            sink.val(" and ");
            if (minDiff == 0) {
                sink.val("current row");
            } else {
                sink.val(minDiff).val(" preceding");
            }
            sink.val(')');
        }

        @Override
        public void toTop() {
            super.toTop();
            nthValue.ofRawNull();
            capacity = initialCapacity;
            memory.truncate();
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            frameSize = 0;
            size = 0;
            freeList.clear();
        }
    }

    public static class Decimal128NthValueOverRowsFrameFunction extends BaseWindowFunction implements Reopenable, WindowDecimal128Function {
        protected final MemoryARW buffer;
        protected final int bufferSize;
        protected final int excludeCount;
        protected final boolean frameIncludesCurrentValue;
        protected final int frameSize;
        protected final int n;
        protected final Decimal128 nthValue = new Decimal128();
        protected final Decimal128 scratch = new Decimal128();
        protected final int type;
        protected long count = 0;
        protected int loIdx = 0;

        public Decimal128NthValueOverRowsFrameFunction(Function arg, long rowsLo, long rowsHi, MemoryARW memory, int n, int type) {
            super(arg);
            assert rowsLo > Long.MIN_VALUE;
            frameSize = (int) (rowsHi - rowsLo + (rowsHi < 0 ? 1 : 0));
            bufferSize = (int) Math.abs(rowsLo);
            excludeCount = rowsHi < 0 ? (int) Math.abs(rowsHi) : 0;
            frameIncludesCurrentValue = rowsHi == 0;
            this.buffer = memory;
            this.n = n;
            this.type = type;
            initBuffer();
        }

        @Override
        public void close() {
            super.close();
            buffer.close();
        }

        @Override
        public void computeNext(Record record) {
            arg.getDecimal128(record, scratch);

            long effectiveCount = Math.min(count, bufferSize);
            long currentFrameElements;
            if (frameIncludesCurrentValue) {
                currentFrameElements = Math.min(effectiveCount, frameSize);
            } else {
                currentFrameElements = Math.clamp(count + 1 - excludeCount, 0, frameSize);
            }

            if (currentFrameElements > 0 || (count == 0 && frameIncludesCurrentValue)) {
                long frameStartIdx = (loIdx + bufferSize - effectiveCount) % bufferSize;
                if (frameIncludesCurrentValue) {
                    if (n <= currentFrameElements) {
                        long nthIdx = (frameStartIdx + n - 1) % bufferSize;
                        buffer.getDecimal128(nthIdx * 16L, nthValue);
                    } else if (n == currentFrameElements + 1) {
                        nthValue.copyFrom(scratch);
                    } else {
                        nthValue.ofRawNull();
                    }
                } else {
                    if (n <= currentFrameElements) {
                        long nthIdx = (frameStartIdx + n - 1) % bufferSize;
                        buffer.getDecimal128(nthIdx * 16L, nthValue);
                    } else {
                        nthValue.ofRawNull();
                    }
                }
            } else {
                nthValue.ofRawNull();
            }

            count = Math.min(count + 1, bufferSize);
            buffer.putDecimal128((long) loIdx * 16L, scratch.getHigh(), scratch.getLow());
            loIdx = (loIdx + 1) % bufferSize;
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            sink.copyFrom(nthValue);
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
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            long addr = spi.getAddress(recordOffset, columnIndex);
            Unsafe.putLong(addr, nthValue.getHigh());
            Unsafe.putLong(addr + Long.BYTES, nthValue.getLow());
        }

        @Override
        public void reopen() {
            nthValue.ofRawNull();
            loIdx = 0;
            initBuffer();
            count = 0;
        }

        @Override
        public void reset() {
            super.reset();
            buffer.close();
            nthValue.ofRawNull();
            loIdx = 0;
            count = 0;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(',').val(n).val(')');
            sink.val(" over (");
            sink.val(" rows between ");
            sink.val(bufferSize);
            sink.val(" preceding and ");
            if (frameIncludesCurrentValue) {
                sink.val("current row");
            } else {
                sink.val(excludeCount).val(" preceding");
            }
            sink.val(')');
        }

        @Override
        public void toTop() {
            super.toTop();
            nthValue.ofRawNull();
            loIdx = 0;
            initBuffer();
            count = 0;
        }

        private void initBuffer() {
            for (int i = 0; i < bufferSize; i++) {
                buffer.putLong((long) i * 16L, Decimals.DECIMAL128_HI_NULL);
                buffer.putLong((long) i * 16L + Long.BYTES, Decimals.DECIMAL128_LO_NULL);
            }
        }
    }

    public static class Decimal128NthValueOverRowsFrameUnboundedFunction extends BaseWindowFunction implements Reopenable, WindowDecimal128Function {
        protected final int bufferSize;
        protected final Decimal128 lockedValue = new Decimal128();
        protected final int n;
        protected final Decimal128 nthValue = new Decimal128();
        protected final Decimal128 scratch = new Decimal128();
        protected final int type;
        protected long totalCount = 0;

        public Decimal128NthValueOverRowsFrameUnboundedFunction(Function arg, long rowsHi, int n, int type) {
            super(arg);
            assert rowsHi < 0 && rowsHi != Long.MIN_VALUE;
            this.bufferSize = (int) Math.abs(rowsHi);
            this.n = n;
            this.type = type;
            lockedValue.ofRawNull();
            nthValue.ofRawNull();
        }

        @Override
        public void computeNext(Record record) {
            arg.getDecimal128(record, scratch);
            totalCount++;
            if (totalCount == n) {
                lockedValue.copyFrom(scratch);
            }
            if (totalCount >= (long) n + bufferSize) {
                nthValue.copyFrom(lockedValue);
            } else {
                nthValue.ofRawNull();
            }
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            sink.copyFrom(nthValue);
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
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            long addr = spi.getAddress(recordOffset, columnIndex);
            Unsafe.putLong(addr, nthValue.getHigh());
            Unsafe.putLong(addr + Long.BYTES, nthValue.getLow());
        }

        @Override
        public void reopen() {
            nthValue.ofRawNull();
            lockedValue.ofRawNull();
            totalCount = 0;
        }

        @Override
        public void reset() {
            super.reset();
            nthValue.ofRawNull();
            lockedValue.ofRawNull();
            totalCount = 0;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(',').val(n).val(')');
            sink.val(" over (");
            sink.val(" rows between unbounded preceding and ");
            sink.val(bufferSize).val(" preceding");
            sink.val(')');
        }

        @Override
        public void toTop() {
            super.toTop();
            nthValue.ofRawNull();
            lockedValue.ofRawNull();
            totalCount = 0;
        }
    }

    static class Decimal128NthValueOverUnboundedPartitionFrameFunction extends BasePartitionedWindowFunction implements WindowDecimal128Function {

        protected final boolean isRange;
        protected final int n;
        protected final Decimal128 scratch = new Decimal128();
        protected final int type;
        protected final Decimal128 value = new Decimal128();

        public Decimal128NthValueOverUnboundedPartitionFrameFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, int n, boolean isRange, int type) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.n = n;
            this.isRange = isRange;
            this.type = type;
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mapValue = key.createValue();

            if (mapValue.isNew()) {
                if (n == 1) {
                    arg.getDecimal128(record, scratch);
                    mapValue.putDecimal128(0, scratch);
                    value.copyFrom(scratch);
                } else {
                    scratch.ofRawNull();
                    mapValue.putDecimal128(0, scratch);
                    value.ofRawNull();
                }
                mapValue.putLong(2, 1);
            } else {
                long count = mapValue.getLong(2);
                if (count >= n) {
                    mapValue.getDecimal128(0, value);
                } else {
                    count++;
                    mapValue.putLong(2, count);
                    if (count == n) {
                        arg.getDecimal128(record, scratch);
                        mapValue.putDecimal128(0, scratch);
                        value.copyFrom(scratch);
                    } else {
                        value.ofRawNull();
                    }
                }
            }
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            sink.copyFrom(value);
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

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(',').val(n).val(')');
            sink.val(" over (");
            sink.val("partition by ");
            sink.val(partitionByRecord.getFunctions());
            sink.val(isRange ? " range" : " rows");
            sink.val(" between unbounded preceding and current row)");
        }
    }

    public static class Decimal128NthValueOverUnboundedRowsFrameFunction extends BaseWindowFunction implements WindowDecimal128Function {
        protected final int n;
        protected final int type;
        protected final Decimal128 value = new Decimal128();
        protected long count;
        protected boolean isFound;

        public Decimal128NthValueOverUnboundedRowsFrameFunction(Function arg, int n, int type) {
            super(arg);
            this.n = n;
            this.type = type;
            value.ofRawNull();
        }

        @Override
        public void computeNext(Record record) {
            if (!isFound) {
                count++;
                if (count == n) {
                    arg.getDecimal128(record, value);
                    isFound = true;
                }
            }
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            sink.copyFrom(value);
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

        @Override
        public void reset() {
            super.reset();
            count = 0;
            isFound = false;
            value.ofRawNull();
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(',').val(n).val(')');
            sink.val(" over (rows between unbounded preceding and current row)");
        }

        @Override
        public void toTop() {
            super.toTop();
            count = 0;
            isFound = false;
            value.ofRawNull();
        }
    }

    public static class Decimal128NthValueOverWholeResultSetFunction extends BaseWindowFunction implements WindowDecimal128Function {
        protected final int n;
        protected final int type;
        protected final Decimal128 value = new Decimal128();
        protected long count;
        protected boolean isFound;

        public Decimal128NthValueOverWholeResultSetFunction(Function arg, int n, int type) {
            super(arg);
            this.n = n;
            this.type = type;
            value.ofRawNull();
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            sink.copyFrom(value);
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public int getPassCount() {
            return WindowFunction.TWO_PASS;
        }

        @Override
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            if (!isFound) {
                count++;
                if (count == n) {
                    arg.getDecimal128(record, value);
                    isFound = true;
                }
            }
        }

        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            long addr = spi.getAddress(recordOffset, columnIndex);
            Unsafe.putLong(addr, value.getHigh());
            Unsafe.putLong(addr + Long.BYTES, value.getLow());
        }

        @Override
        public void reset() {
            super.reset();
            clearState();
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(',').val(n).val(')');
            sink.val(" over ()");
        }

        @Override
        public void toTop() {
            super.toTop();
            clearState();
        }

        private void clearState() {
            count = 0;
            isFound = false;
            value.ofRawNull();
        }
    }

    private Function newInstanceDecimal32(
            int position,
            ObjList<Function> args,
            CairoConfiguration configuration,
            WindowContext windowContext,
            int n,
            int argType
    ) throws SqlException {
        long rowsLo = windowContext.getRowsLo();
        long rowsHi = windowContext.getRowsHi();
        if (windowContext.getFramingMode() == WindowExpression.FRAMING_ROWS) {
            if (rowsLo != Long.MIN_VALUE && Math.abs(rowsLo) > Integer.MAX_VALUE) {
                throw SqlException.$(windowContext.getRowsLoKindPos(), "frame start exceeds maximum supported size");
            }
            if (rowsHi != Long.MAX_VALUE && Math.abs(rowsHi) > Integer.MAX_VALUE) {
                throw SqlException.$(windowContext.getRowsHiKindPos(), "frame end exceeds maximum supported size");
            }
        }

        int framingMode = windowContext.getFramingMode();
        RecordSink partitionBySink = windowContext.getPartitionBySink();
        ColumnTypes partitionByKeyTypes = windowContext.getPartitionByKeyTypes();
        VirtualRecord partitionByRecord = windowContext.getPartitionByRecord();

        if (partitionByRecord != null) {
            if (framingMode == WindowExpression.FRAMING_RANGE) {
                if (windowContext.isDefaultFrame() && (!windowContext.isOrdered() || windowContext.getRowsHi() == Long.MAX_VALUE)) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, NTH_VALUE_DECIMAL64_TYPES);
                    try {
                        return new Decimal32NthValueOverPartitionFunction(map, partitionByRecord, partitionBySink, args.get(0), n, argType);
                    } catch (Throwable t) {
                        Misc.free(map);
                        throw t;
                    }
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, NTH_VALUE_DECIMAL64_TYPES);
                    try {
                        return new Decimal32NthValueOverUnboundedPartitionFrameFunction(map, partitionByRecord, partitionBySink, args.get(0), n, true, argType);
                    } catch (Throwable t) {
                        Misc.free(map);
                        throw t;
                    }
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    ArrayColumnTypes columnTypes = new ArrayColumnTypes();
                    columnTypes.add(ColumnType.LONG);
                    columnTypes.add(ColumnType.LONG);
                    columnTypes.add(ColumnType.LONG);
                    columnTypes.add(ColumnType.LONG);
                    columnTypes.add(ColumnType.LONG);
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, columnTypes);
                    final int initialBufferSize = configuration.getSqlWindowInitialRangeBufferSize();
                    MemoryARW mem;
                    try {
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                    } catch (Throwable t) {
                        Misc.free(map);
                        throw t;
                    }
                    try {
                        return new Decimal32NthValueOverPartitionRangeFrameFunction(map, partitionByRecord, partitionBySink,
                                rowsLo, rowsHi, args.get(0), mem, initialBufferSize, timestampIndex, n, argType);
                    } catch (Throwable t) {
                        Misc.free(map);
                        Misc.free(mem);
                        throw t;
                    }
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, NTH_VALUE_DECIMAL64_TYPES);
                    try {
                        return new Decimal32NthValueOverUnboundedPartitionFrameFunction(map, partitionByRecord, partitionBySink, args.get(0), n, false, argType);
                    } catch (Throwable t) {
                        Misc.free(map);
                        throw t;
                    }
                } else if (rowsLo == 0 && rowsHi == 0) {
                    return new Decimal32NthValueOverCurrentRowFunction(args.get(0), n, argType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, NTH_VALUE_DECIMAL64_TYPES);
                    try {
                        return new Decimal32NthValueOverPartitionFunction(map, partitionByRecord, partitionBySink, args.get(0), n, argType);
                    } catch (Throwable t) {
                        Misc.free(map);
                        throw t;
                    }
                } else if (rowsLo == Long.MIN_VALUE) {
                    ArrayColumnTypes columnTypes = new ArrayColumnTypes();
                    columnTypes.add(ColumnType.LONG);
                    columnTypes.add(ColumnType.LONG);
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, columnTypes);
                    try {
                        return new Decimal32NthValueOverPartitionRowsFrameUnboundedFunction(map, partitionByRecord, partitionBySink, rowsHi, args.get(0), n, argType);
                    } catch (Throwable t) {
                        Misc.free(map);
                        throw t;
                    }
                } else {
                    ArrayColumnTypes columnTypes = new ArrayColumnTypes();
                    columnTypes.add(ColumnType.LONG);
                    columnTypes.add(ColumnType.LONG);
                    columnTypes.add(ColumnType.LONG);
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, columnTypes);
                    MemoryARW mem;
                    try {
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                    } catch (Throwable t) {
                        Misc.free(map);
                        throw t;
                    }
                    try {
                        return new Decimal32NthValueOverPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink,
                                rowsLo, rowsHi, args.get(0), mem, n, argType);
                    } catch (Throwable t) {
                        Misc.free(map);
                        Misc.free(mem);
                        throw t;
                    }
                }
            }
        } else {
            if (framingMode == WindowExpression.FRAMING_RANGE) {
                if (!windowContext.isOrdered() && windowContext.isDefaultFrame()) {
                    return new Decimal32NthValueOverWholeResultSetFunction(args.get(0), n, argType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    return new Decimal32NthValueOverWholeResultSetFunction(args.get(0), n, argType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    return new Decimal32NthValueOverUnboundedRowsFrameFunction(args.get(0), n, argType);
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    return new Decimal32NthValueOverRangeFrameFunction(rowsLo, rowsHi, args.get(0), configuration, timestampIndex, n, argType);
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    return new Decimal32NthValueOverWholeResultSetFunction(args.get(0), n, argType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    return new Decimal32NthValueOverUnboundedRowsFrameFunction(args.get(0), n, argType);
                } else if (rowsLo == 0 && rowsHi == 0) {
                    return new Decimal32NthValueOverCurrentRowFunction(args.get(0), n, argType);
                } else if (rowsLo == Long.MIN_VALUE) {
                    return new Decimal32NthValueOverRowsFrameUnboundedFunction(args.get(0), rowsHi, n, argType);
                } else {
                    MemoryARW mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                            configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                    try {
                        return new Decimal32NthValueOverRowsFrameFunction(args.get(0), rowsLo, rowsHi, mem, n, argType);
                    } catch (Throwable t) {
                        Misc.free(mem);
                        throw t;
                    }
                }
            }
        }

        throw SqlException.$(position, "function not implemented for given window parameters");
    }

    static class Decimal32NthValueOverCurrentRowFunction extends BaseWindowFunction implements WindowDecimal32Function {

        private final int n;
        private final int type;
        private int value;

        Decimal32NthValueOverCurrentRowFunction(Function arg, int n, int type) {
            super(arg);
            this.n = n;
            this.type = type;
        }

        @Override
        public void computeNext(Record record) {
            value = n == 1 ? arg.getDecimal32(record) : Decimals.DECIMAL32_NULL;
        }

        @Override
        public int getDecimal32(Record rec) {
            return value;
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
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putInt(spi.getAddress(recordOffset, columnIndex), value);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(',').val(n).val(')');
            sink.val(" over (rows between current row and current row)");
        }
    }

    static class Decimal32NthValueOverPartitionFunction extends BasePartitionedWindowFunction implements WindowDecimal32Function {

        private final int n;
        private final int type;

        public Decimal32NthValueOverPartitionFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, int n, int type) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.n = n;
            this.type = type;
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public int getPassCount() {
            return WindowFunction.TWO_PASS;
        }

        @Override
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.createValue();

            if (value.isNew()) {
                if (n == 1) {
                    value.putLong(0, arg.getDecimal32(record));
                } else {
                    value.putLong(0, Decimals.DECIMAL32_NULL);
                }
                value.putLong(1, 1);
            } else {
                long count = value.getLong(1) + 1;
                value.putLong(1, count);
                if (count == n) {
                    value.putLong(0, arg.getDecimal32(record));
                }
            }
        }

        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.findValue();
            assert value != null;
            Unsafe.putInt(spi.getAddress(recordOffset, columnIndex), (int) value.getLong(0));
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(',').val(n).val(')');
            sink.val(" over (");
            sink.val("partition by ");
            sink.val(partitionByRecord.getFunctions());
            sink.val(')');
        }
    }

    public static class Decimal32NthValueOverPartitionRangeFrameFunction extends BasePartitionedWindowFunction implements WindowDecimal32Function {

        protected static final int RECORD_SIZE = Long.BYTES + Integer.BYTES;
        protected final boolean frameIncludesCurrentValue;
        protected final boolean frameLoBounded;
        protected final LongList freeList = new LongList();
        protected final int initialBufferSize;
        protected final long maxDiff;
        protected final MemoryARW memory;
        protected final RingBufferDesc memoryDesc = new RingBufferDesc();
        protected final long minDiff;
        protected final int n;
        protected final int timestampIndex;
        protected final int type;
        protected int nthValue;

        public Decimal32NthValueOverPartitionRangeFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rangeLo,
                long rangeHi,
                Function arg,
                MemoryARW memory,
                int initialBufferSize,
                int timestampIdx,
                int n,
                int type
        ) {
            super(map, partitionByRecord, partitionBySink, arg);
            frameLoBounded = rangeLo != Long.MIN_VALUE;
            maxDiff = frameLoBounded ? Math.abs(rangeLo) : Math.abs(rangeHi);
            minDiff = Math.abs(rangeHi);
            this.memory = memory;
            this.initialBufferSize = initialBufferSize;
            this.timestampIndex = timestampIdx;
            this.n = n;
            this.frameIncludesCurrentValue = rangeHi == 0;
            this.type = type;
        }

        @Override
        public void close() {
            super.close();
            memory.close();
            freeList.clear();
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mapValue = key.createValue();

            long frameSize;
            long startOffset;
            long size;
            long capacity;
            long firstIdx;
            long timestamp = record.getTimestamp(timestampIndex);
            int v = arg.getDecimal32(record);

            if (mapValue.isNew()) {
                capacity = initialBufferSize;
                startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
                firstIdx = 0;
                memory.putLong(startOffset, timestamp);
                memory.putInt(startOffset + Long.BYTES, v);
                size = 1;
                if (frameIncludesCurrentValue) {
                    frameSize = 1;
                    nthValue = n == 1 ? v : Decimals.DECIMAL32_NULL;
                } else {
                    frameSize = 0;
                    nthValue = Decimals.DECIMAL32_NULL;
                }
            } else {
                frameSize = mapValue.getLong(0);
                startOffset = mapValue.getLong(1);
                size = mapValue.getLong(2);
                capacity = mapValue.getLong(3);
                firstIdx = mapValue.getLong(4);

                if (!frameLoBounded && frameSize >= n) {
                    long nthIdx = (firstIdx + n - 1) % capacity;
                    nthValue = memory.getInt(startOffset + nthIdx * RECORD_SIZE + Long.BYTES);
                    return;
                }

                long newFirstIdx = firstIdx;
                if (frameLoBounded) {
                    for (long i = 0, nn = size; i < nn; i++) {
                        long idx = (firstIdx + i) % capacity;
                        long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                        if (Math.abs(timestamp - ts) > maxDiff) {
                            if (frameSize > 0) {
                                frameSize--;
                            }
                            newFirstIdx = (idx + 1) % capacity;
                            size--;
                        } else {
                            break;
                        }
                    }
                }
                firstIdx = newFirstIdx;

                if (size == capacity) {
                    memoryDesc.reset(capacity, startOffset, size, firstIdx, freeList);
                    expandRingBuffer(memory, memoryDesc, RECORD_SIZE);
                    capacity = memoryDesc.capacity;
                    startOffset = memoryDesc.startOffset;
                    firstIdx = memoryDesc.firstIdx;
                }
                memory.putLong(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE, timestamp);
                memory.putInt(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE + Long.BYTES, v);
                size++;

                if (frameLoBounded) {
                    for (long i = frameSize; i < size; i++) {
                        long idx = (firstIdx + i) % capacity;
                        long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                        long diff = Math.abs(ts - timestamp);
                        if (diff <= maxDiff && diff >= minDiff) {
                            frameSize++;
                        } else {
                            break;
                        }
                    }
                } else {
                    for (long i = frameSize; i < size; i++) {
                        long idx = (firstIdx + i) % capacity;
                        long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                        if (Math.abs(timestamp - ts) >= minDiff) {
                            frameSize++;
                        } else {
                            break;
                        }
                    }
                }

                if (frameSize >= n) {
                    long nthIdx = (firstIdx + n - 1) % capacity;
                    nthValue = memory.getInt(startOffset + nthIdx * RECORD_SIZE + Long.BYTES);
                } else {
                    nthValue = Decimals.DECIMAL32_NULL;
                }
            }

            mapValue.putLong(0, frameSize);
            mapValue.putLong(1, startOffset);
            mapValue.putLong(2, size);
            mapValue.putLong(3, capacity);
            mapValue.putLong(4, firstIdx);
        }

        @Override
        public int getDecimal32(Record rec) {
            return nthValue;
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
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putInt(spi.getAddress(recordOffset, columnIndex), nthValue);
        }

        @Override
        public void reopen() {
            super.reopen();
            freeList.clear();
            nthValue = Decimals.DECIMAL32_NULL;
        }

        @Override
        public void reset() {
            super.reset();
            memory.close();
            freeList.clear();
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(',').val(n).val(')');
            sink.val(" over (");
            sink.val("partition by ");
            sink.val(partitionByRecord.getFunctions());
            sink.val(" range between ");
            if (frameLoBounded) {
                sink.val(maxDiff).val(" preceding");
            } else {
                sink.val("unbounded preceding");
            }
            sink.val(" and ");
            if (minDiff == 0) {
                sink.val("current row");
            } else {
                sink.val(minDiff).val(" preceding");
            }
            sink.val(')');
        }

        @Override
        public void toTop() {
            super.toTop();
            memory.truncate();
            freeList.clear();
        }
    }

    public static class Decimal32NthValueOverPartitionRowsFrameFunction extends BasePartitionedWindowFunction implements WindowDecimal32Function {

        protected final int bufferSize;
        protected final int excludeCount;
        protected final boolean frameIncludesCurrentValue;
        protected final int frameSize;
        protected final MemoryARW memory;
        protected final int n;
        protected final int type;
        protected int nthValue;

        public Decimal32NthValueOverPartitionRowsFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rowsLo,
                long rowsHi,
                Function arg,
                MemoryARW memory,
                int n,
                int type
        ) {
            super(map, partitionByRecord, partitionBySink, arg);
            assert rowsLo > Long.MIN_VALUE;
            frameSize = (int) (rowsHi - rowsLo + (rowsHi < 0 ? 1 : 0));
            bufferSize = (int) Math.abs(rowsLo);
            this.excludeCount = rowsHi < 0 ? (int) Math.abs(rowsHi) : 0;
            this.frameIncludesCurrentValue = rowsHi == 0;
            this.memory = memory;
            this.n = n;
            this.type = type;
        }

        @Override
        public void close() {
            super.close();
            memory.close();
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mapValue = key.createValue();

            int v = arg.getDecimal32(record);

            long loIdx;
            long startOffset;
            long count;

            if (mapValue.isNew()) {
                loIdx = 0;
                count = 0;
                startOffset = memory.appendAddressFor((long) bufferSize * Integer.BYTES) - memory.getPageAddress(0);
                mapValue.putLong(1, startOffset);
                for (int i = 0; i < bufferSize; i++) {
                    memory.putInt(startOffset + (long) i * Integer.BYTES, Decimals.DECIMAL32_NULL);
                }
            } else {
                loIdx = mapValue.getLong(0);
                startOffset = mapValue.getLong(1);
                count = mapValue.getLong(2);
            }

            long effectiveCount = Math.min(count, bufferSize);
            long currentFrameSize;
            if (frameIncludesCurrentValue) {
                currentFrameSize = Math.min(effectiveCount, frameSize);
            } else {
                currentFrameSize = Math.clamp(count + 1 - excludeCount, 0, frameSize);
            }

            if (currentFrameSize > 0 || (count == 0 && frameIncludesCurrentValue)) {
                long frameStartIdx = (loIdx + bufferSize - effectiveCount) % bufferSize;
                if (frameIncludesCurrentValue) {
                    if (n <= currentFrameSize) {
                        long nthIdx = (frameStartIdx + n - 1) % bufferSize;
                        nthValue = memory.getInt(startOffset + nthIdx * Integer.BYTES);
                    } else if (n == currentFrameSize + 1) {
                        nthValue = v;
                    } else {
                        nthValue = Decimals.DECIMAL32_NULL;
                    }
                } else {
                    if (n <= currentFrameSize) {
                        long nthIdx = (frameStartIdx + n - 1) % bufferSize;
                        nthValue = memory.getInt(startOffset + nthIdx * Integer.BYTES);
                    } else {
                        nthValue = Decimals.DECIMAL32_NULL;
                    }
                }
            } else {
                nthValue = Decimals.DECIMAL32_NULL;
            }

            count = Math.min(count + 1, bufferSize);
            mapValue.putLong(0, (loIdx + 1) % bufferSize);
            mapValue.putLong(2, count);
            memory.putInt(startOffset + loIdx * Integer.BYTES, v);
        }

        @Override
        public int getDecimal32(Record rec) {
            return nthValue;
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
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putInt(spi.getAddress(recordOffset, columnIndex), nthValue);
        }

        @Override
        public void reopen() {
            super.reopen();
        }

        @Override
        public void reset() {
            super.reset();
            memory.close();
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(',').val(n).val(')');
            sink.val(" over (");
            sink.val("partition by ");
            sink.val(partitionByRecord.getFunctions());
            sink.val(" rows between ");
            sink.val(bufferSize);
            sink.val(" preceding and ");
            if (frameIncludesCurrentValue) {
                sink.val("current row");
            } else {
                sink.val(excludeCount).val(" preceding");
            }
            sink.val(')');
        }

        @Override
        public void toTop() {
            super.toTop();
            memory.truncate();
        }
    }

    public static class Decimal32NthValueOverPartitionRowsFrameUnboundedFunction extends BasePartitionedWindowFunction implements WindowDecimal32Function {

        protected final int bufferSize;
        protected final int n;
        protected final int type;
        protected int nthValue;

        public Decimal32NthValueOverPartitionRowsFrameUnboundedFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rowsHi,
                Function arg,
                int n,
                int type
        ) {
            super(map, partitionByRecord, partitionBySink, arg);
            assert rowsHi < 0 && rowsHi != Long.MIN_VALUE;
            this.bufferSize = (int) Math.abs(rowsHi);
            this.n = n;
            this.type = type;
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mapValue = key.createValue();

            int v = arg.getDecimal32(record);
            long count;
            if (mapValue.isNew()) {
                count = 0;
                mapValue.putLong(1, Decimals.DECIMAL32_NULL);
            } else {
                count = mapValue.getLong(0);
            }
            count++;
            if (count == n) {
                mapValue.putLong(1, v);
            }
            if (count >= (long) n + bufferSize) {
                nthValue = (int) mapValue.getLong(1);
            } else {
                nthValue = Decimals.DECIMAL32_NULL;
            }
            mapValue.putLong(0, count);
        }

        @Override
        public int getDecimal32(Record rec) {
            return nthValue;
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
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putInt(spi.getAddress(recordOffset, columnIndex), nthValue);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(',').val(n).val(')');
            sink.val(" over (");
            sink.val("partition by ");
            sink.val(partitionByRecord.getFunctions());
            sink.val(" rows between unbounded preceding and ");
            sink.val(bufferSize).val(" preceding");
            sink.val(')');
        }
    }

    public static class Decimal32NthValueOverRangeFrameFunction extends BaseWindowFunction implements Reopenable, WindowDecimal32Function {
        protected static final int RECORD_SIZE = Long.BYTES + Integer.BYTES;
        protected final boolean frameIncludesCurrentValue;
        protected final boolean frameLoBounded;
        protected final LongList freeList = new LongList();
        protected final long initialCapacity;
        protected final long maxDiff;
        protected final MemoryARW memory;
        protected final RingBufferDesc memoryDesc = new RingBufferDesc();
        protected final long minDiff;
        protected final int n;
        protected final int timestampIndex;
        protected final int type;
        protected long capacity;
        protected long firstIdx;
        protected long frameSize;
        protected int nthValue;
        protected long size;
        protected long startOffset;

        public Decimal32NthValueOverRangeFrameFunction(
                long rangeLo,
                long rangeHi,
                Function arg,
                CairoConfiguration configuration,
                int timestampIdx,
                int n,
                int type
        ) {
            super(arg);
            frameLoBounded = rangeLo != Long.MIN_VALUE;
            maxDiff = frameLoBounded ? Math.abs(rangeLo) : Math.abs(rangeHi);
            minDiff = Math.abs(rangeHi);
            timestampIndex = timestampIdx;
            initialCapacity = configuration.getSqlWindowStorePageSize() / RECORD_SIZE;
            capacity = initialCapacity;
            MemoryARW mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                    configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
            try {
                startOffset = mem.appendAddressFor(capacity * RECORD_SIZE) - mem.getPageAddress(0);
            } catch (Throwable t) {
                Misc.free(mem);
                throw t;
            }
            memory = mem;
            firstIdx = 0;
            frameSize = 0;
            frameIncludesCurrentValue = rangeHi == 0;
            this.n = n;
            this.type = type;
        }

        @Override
        public void close() {
            super.close();
            memory.close();
        }

        @Override
        public void computeNext(Record record) {
            if (!frameLoBounded && frameSize >= n) {
                long nthIdx = (firstIdx + n - 1) % capacity;
                nthValue = memory.getInt(startOffset + nthIdx * RECORD_SIZE + Long.BYTES);
                return;
            }

            long timestamp = record.getTimestamp(timestampIndex);
            int v = arg.getDecimal32(record);
            long newFirstIdx = firstIdx;

            if (frameLoBounded) {
                for (long i = 0, nn = size; i < nn; i++) {
                    long idx = (firstIdx + i) % capacity;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    if (Math.abs(timestamp - ts) > maxDiff) {
                        if (frameSize > 0) {
                            frameSize--;
                        }
                        newFirstIdx = (idx + 1) % capacity;
                        size--;
                    } else {
                        break;
                    }
                }
            }
            firstIdx = newFirstIdx;

            if (size == capacity) {
                memoryDesc.reset(capacity, startOffset, size, firstIdx, freeList);
                expandRingBuffer(memory, memoryDesc, RECORD_SIZE);
                capacity = memoryDesc.capacity;
                startOffset = memoryDesc.startOffset;
                firstIdx = memoryDesc.firstIdx;
            }
            memory.putLong(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE, timestamp);
            memory.putInt(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE + Long.BYTES, v);
            size++;

            if (frameLoBounded) {
                for (long i = frameSize, nn = size; i < nn; i++) {
                    long idx = (firstIdx + i) % capacity;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    long diff = Math.abs(ts - timestamp);
                    if (diff <= maxDiff && diff >= minDiff) {
                        frameSize++;
                    } else {
                        break;
                    }
                }
            } else {
                for (long i = frameSize, nn = size; i < nn; i++) {
                    long idx = (firstIdx + i) % capacity;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    if (Math.abs(timestamp - ts) >= minDiff) {
                        frameSize++;
                    } else {
                        break;
                    }
                }
            }

            if (frameSize >= n) {
                long nthIdx = (firstIdx + n - 1) % capacity;
                nthValue = memory.getInt(startOffset + nthIdx * RECORD_SIZE + Long.BYTES);
            } else {
                nthValue = Decimals.DECIMAL32_NULL;
            }
        }

        @Override
        public int getDecimal32(Record rec) {
            return nthValue;
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
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putInt(spi.getAddress(recordOffset, columnIndex), nthValue);
        }

        @Override
        public void reopen() {
            nthValue = Decimals.DECIMAL32_NULL;
            capacity = initialCapacity;
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            frameSize = 0;
            size = 0;
            freeList.clear();
        }

        @Override
        public void reset() {
            super.reset();
            memory.close();
            freeList.clear();
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(',').val(n).val(')');
            sink.val(" over (");
            sink.val("range between ");
            if (frameLoBounded) {
                sink.val(maxDiff).val(" preceding");
            } else {
                sink.val("unbounded preceding");
            }
            sink.val(" and ");
            if (minDiff == 0) {
                sink.val("current row");
            } else {
                sink.val(minDiff).val(" preceding");
            }
            sink.val(')');
        }

        @Override
        public void toTop() {
            super.toTop();
            nthValue = Decimals.DECIMAL32_NULL;
            capacity = initialCapacity;
            memory.truncate();
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            frameSize = 0;
            size = 0;
            freeList.clear();
        }
    }

    public static class Decimal32NthValueOverRowsFrameFunction extends BaseWindowFunction implements Reopenable, WindowDecimal32Function {
        protected final MemoryARW buffer;
        protected final int bufferSize;
        protected final int excludeCount;
        protected final boolean frameIncludesCurrentValue;
        protected final int frameSize;
        protected final int n;
        protected final int type;
        protected long count = 0;
        protected int loIdx = 0;
        protected int nthValue;

        public Decimal32NthValueOverRowsFrameFunction(Function arg, long rowsLo, long rowsHi, MemoryARW memory, int n, int type) {
            super(arg);
            assert rowsLo > Long.MIN_VALUE;
            frameSize = (int) (rowsHi - rowsLo + (rowsHi < 0 ? 1 : 0));
            bufferSize = (int) Math.abs(rowsLo);
            excludeCount = rowsHi < 0 ? (int) Math.abs(rowsHi) : 0;
            frameIncludesCurrentValue = rowsHi == 0;
            this.buffer = memory;
            this.n = n;
            this.type = type;
            initBuffer();
        }

        @Override
        public void close() {
            super.close();
            buffer.close();
        }

        @Override
        public void computeNext(Record record) {
            int v = arg.getDecimal32(record);

            long effectiveCount = Math.min(count, bufferSize);
            long currentFrameElements;
            if (frameIncludesCurrentValue) {
                currentFrameElements = Math.min(effectiveCount, frameSize);
            } else {
                currentFrameElements = Math.clamp(count + 1 - excludeCount, 0, frameSize);
            }

            if (currentFrameElements > 0 || (count == 0 && frameIncludesCurrentValue)) {
                long frameStartIdx = (loIdx + bufferSize - effectiveCount) % bufferSize;
                if (frameIncludesCurrentValue) {
                    if (n <= currentFrameElements) {
                        long nthIdx = (frameStartIdx + n - 1) % bufferSize;
                        nthValue = buffer.getInt(nthIdx * Integer.BYTES);
                    } else if (n == currentFrameElements + 1) {
                        nthValue = v;
                    } else {
                        nthValue = Decimals.DECIMAL32_NULL;
                    }
                } else {
                    if (n <= currentFrameElements) {
                        long nthIdx = (frameStartIdx + n - 1) % bufferSize;
                        nthValue = buffer.getInt(nthIdx * Integer.BYTES);
                    } else {
                        nthValue = Decimals.DECIMAL32_NULL;
                    }
                }
            } else {
                nthValue = Decimals.DECIMAL32_NULL;
            }

            count = Math.min(count + 1, bufferSize);
            buffer.putInt((long) loIdx * Integer.BYTES, v);
            loIdx = (loIdx + 1) % bufferSize;
        }

        @Override
        public int getDecimal32(Record rec) {
            return nthValue;
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
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putInt(spi.getAddress(recordOffset, columnIndex), nthValue);
        }

        @Override
        public void reopen() {
            nthValue = Decimals.DECIMAL32_NULL;
            loIdx = 0;
            initBuffer();
            count = 0;
        }

        @Override
        public void reset() {
            super.reset();
            buffer.close();
            nthValue = Decimals.DECIMAL32_NULL;
            loIdx = 0;
            count = 0;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(',').val(n).val(')');
            sink.val(" over (");
            sink.val(" rows between ");
            sink.val(bufferSize);
            sink.val(" preceding and ");
            if (frameIncludesCurrentValue) {
                sink.val("current row");
            } else {
                sink.val(excludeCount).val(" preceding");
            }
            sink.val(')');
        }

        @Override
        public void toTop() {
            super.toTop();
            nthValue = Decimals.DECIMAL32_NULL;
            loIdx = 0;
            initBuffer();
            count = 0;
        }

        private void initBuffer() {
            for (int i = 0; i < bufferSize; i++) {
                buffer.putInt((long) i * Integer.BYTES, Decimals.DECIMAL32_NULL);
            }
        }
    }

    public static class Decimal32NthValueOverRowsFrameUnboundedFunction extends BaseWindowFunction implements Reopenable, WindowDecimal32Function {
        protected final int bufferSize;
        protected final int n;
        protected final int type;
        protected int lockedValue = Decimals.DECIMAL32_NULL;
        protected int nthValue = Decimals.DECIMAL32_NULL;
        protected long totalCount = 0;

        public Decimal32NthValueOverRowsFrameUnboundedFunction(Function arg, long rowsHi, int n, int type) {
            super(arg);
            assert rowsHi < 0 && rowsHi != Long.MIN_VALUE;
            this.bufferSize = (int) Math.abs(rowsHi);
            this.n = n;
            this.type = type;
        }

        @Override
        public void computeNext(Record record) {
            int v = arg.getDecimal32(record);
            totalCount++;
            if (totalCount == n) {
                lockedValue = v;
            }
            nthValue = totalCount >= (long) n + bufferSize ? lockedValue : Decimals.DECIMAL32_NULL;
        }

        @Override
        public int getDecimal32(Record rec) {
            return nthValue;
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
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putInt(spi.getAddress(recordOffset, columnIndex), nthValue);
        }

        @Override
        public void reopen() {
            nthValue = Decimals.DECIMAL32_NULL;
            lockedValue = Decimals.DECIMAL32_NULL;
            totalCount = 0;
        }

        @Override
        public void reset() {
            super.reset();
            nthValue = Decimals.DECIMAL32_NULL;
            lockedValue = Decimals.DECIMAL32_NULL;
            totalCount = 0;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(',').val(n).val(')');
            sink.val(" over (");
            sink.val(" rows between unbounded preceding and ");
            sink.val(bufferSize).val(" preceding");
            sink.val(')');
        }

        @Override
        public void toTop() {
            super.toTop();
            nthValue = Decimals.DECIMAL32_NULL;
            lockedValue = Decimals.DECIMAL32_NULL;
            totalCount = 0;
        }
    }

    static class Decimal32NthValueOverUnboundedPartitionFrameFunction extends BasePartitionedWindowFunction implements WindowDecimal32Function {

        protected final boolean isRange;
        protected final int n;
        protected final int type;
        protected int value;

        public Decimal32NthValueOverUnboundedPartitionFrameFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, int n, boolean isRange, int type) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.n = n;
            this.isRange = isRange;
            this.type = type;
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mapValue = key.createValue();

            if (mapValue.isNew()) {
                if (n == 1) {
                    int v = arg.getDecimal32(record);
                    mapValue.putLong(0, v);
                    value = v;
                } else {
                    mapValue.putLong(0, Decimals.DECIMAL32_NULL);
                    value = Decimals.DECIMAL32_NULL;
                }
                mapValue.putLong(1, 1);
            } else {
                long count = mapValue.getLong(1);
                if (count >= n) {
                    value = (int) mapValue.getLong(0);
                } else {
                    count++;
                    mapValue.putLong(1, count);
                    if (count == n) {
                        int v = arg.getDecimal32(record);
                        mapValue.putLong(0, v);
                        value = v;
                    } else {
                        value = Decimals.DECIMAL32_NULL;
                    }
                }
            }
        }

        @Override
        public int getDecimal32(Record rec) {
            return value;
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
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putInt(spi.getAddress(recordOffset, columnIndex), value);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(',').val(n).val(')');
            sink.val(" over (");
            sink.val("partition by ");
            sink.val(partitionByRecord.getFunctions());
            sink.val(isRange ? " range" : " rows");
            sink.val(" between unbounded preceding and current row)");
        }
    }

    public static class Decimal32NthValueOverUnboundedRowsFrameFunction extends BaseWindowFunction implements WindowDecimal32Function {
        protected final int n;
        protected final int type;
        protected long count;
        protected boolean isFound;
        protected int value = Decimals.DECIMAL32_NULL;

        public Decimal32NthValueOverUnboundedRowsFrameFunction(Function arg, int n, int type) {
            super(arg);
            this.n = n;
            this.type = type;
        }

        @Override
        public void computeNext(Record record) {
            if (!isFound) {
                count++;
                if (count == n) {
                    value = arg.getDecimal32(record);
                    isFound = true;
                }
            }
        }

        @Override
        public int getDecimal32(Record rec) {
            return value;
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
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putInt(spi.getAddress(recordOffset, columnIndex), value);
        }

        @Override
        public void reset() {
            super.reset();
            count = 0;
            isFound = false;
            value = Decimals.DECIMAL32_NULL;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(',').val(n).val(')');
            sink.val(" over (rows between unbounded preceding and current row)");
        }

        @Override
        public void toTop() {
            super.toTop();
            count = 0;
            isFound = false;
            value = Decimals.DECIMAL32_NULL;
        }
    }

    public static class Decimal32NthValueOverWholeResultSetFunction extends BaseWindowFunction implements WindowDecimal32Function {
        protected final int n;
        protected final int type;
        protected long count;
        protected boolean isFound;
        protected int value = Decimals.DECIMAL32_NULL;

        public Decimal32NthValueOverWholeResultSetFunction(Function arg, int n, int type) {
            super(arg);
            this.n = n;
            this.type = type;
        }

        @Override
        public int getDecimal32(Record rec) {
            return value;
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public int getPassCount() {
            return WindowFunction.TWO_PASS;
        }

        @Override
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            if (!isFound) {
                count++;
                if (count == n) {
                    value = arg.getDecimal32(record);
                    isFound = true;
                }
            }
        }

        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            Unsafe.putInt(spi.getAddress(recordOffset, columnIndex), value);
        }

        @Override
        public void reset() {
            super.reset();
            clearState();
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(',').val(n).val(')');
            sink.val(" over ()");
        }

        @Override
        public void toTop() {
            super.toTop();
            clearState();
        }

        private void clearState() {
            count = 0;
            isFound = false;
            value = Decimals.DECIMAL32_NULL;
        }
    }

    private Function newInstanceDecimal16(
            int position,
            ObjList<Function> args,
            CairoConfiguration configuration,
            WindowContext windowContext,
            int n,
            int argType
    ) throws SqlException {
        long rowsLo = windowContext.getRowsLo();
        long rowsHi = windowContext.getRowsHi();
        if (windowContext.getFramingMode() == WindowExpression.FRAMING_ROWS) {
            if (rowsLo != Long.MIN_VALUE && Math.abs(rowsLo) > Integer.MAX_VALUE) {
                throw SqlException.$(windowContext.getRowsLoKindPos(), "frame start exceeds maximum supported size");
            }
            if (rowsHi != Long.MAX_VALUE && Math.abs(rowsHi) > Integer.MAX_VALUE) {
                throw SqlException.$(windowContext.getRowsHiKindPos(), "frame end exceeds maximum supported size");
            }
        }

        int framingMode = windowContext.getFramingMode();
        RecordSink partitionBySink = windowContext.getPartitionBySink();
        ColumnTypes partitionByKeyTypes = windowContext.getPartitionByKeyTypes();
        VirtualRecord partitionByRecord = windowContext.getPartitionByRecord();

        if (partitionByRecord != null) {
            if (framingMode == WindowExpression.FRAMING_RANGE) {
                if (windowContext.isDefaultFrame() && (!windowContext.isOrdered() || windowContext.getRowsHi() == Long.MAX_VALUE)) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, NTH_VALUE_DECIMAL64_TYPES);
                    try {
                        return new Decimal16NthValueOverPartitionFunction(map, partitionByRecord, partitionBySink, args.get(0), n, argType);
                    } catch (Throwable t) {
                        Misc.free(map);
                        throw t;
                    }
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, NTH_VALUE_DECIMAL64_TYPES);
                    try {
                        return new Decimal16NthValueOverUnboundedPartitionFrameFunction(map, partitionByRecord, partitionBySink, args.get(0), n, true, argType);
                    } catch (Throwable t) {
                        Misc.free(map);
                        throw t;
                    }
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    ArrayColumnTypes columnTypes = new ArrayColumnTypes();
                    columnTypes.add(ColumnType.LONG);
                    columnTypes.add(ColumnType.LONG);
                    columnTypes.add(ColumnType.LONG);
                    columnTypes.add(ColumnType.LONG);
                    columnTypes.add(ColumnType.LONG);
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, columnTypes);
                    final int initialBufferSize = configuration.getSqlWindowInitialRangeBufferSize();
                    MemoryARW mem;
                    try {
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                    } catch (Throwable t) {
                        Misc.free(map);
                        throw t;
                    }
                    try {
                        return new Decimal16NthValueOverPartitionRangeFrameFunction(map, partitionByRecord, partitionBySink,
                                rowsLo, rowsHi, args.get(0), mem, initialBufferSize, timestampIndex, n, argType);
                    } catch (Throwable t) {
                        Misc.free(map);
                        Misc.free(mem);
                        throw t;
                    }
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, NTH_VALUE_DECIMAL64_TYPES);
                    try {
                        return new Decimal16NthValueOverUnboundedPartitionFrameFunction(map, partitionByRecord, partitionBySink, args.get(0), n, false, argType);
                    } catch (Throwable t) {
                        Misc.free(map);
                        throw t;
                    }
                } else if (rowsLo == 0 && rowsHi == 0) {
                    return new Decimal16NthValueOverCurrentRowFunction(args.get(0), n, argType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, NTH_VALUE_DECIMAL64_TYPES);
                    try {
                        return new Decimal16NthValueOverPartitionFunction(map, partitionByRecord, partitionBySink, args.get(0), n, argType);
                    } catch (Throwable t) {
                        Misc.free(map);
                        throw t;
                    }
                } else if (rowsLo == Long.MIN_VALUE) {
                    ArrayColumnTypes columnTypes = new ArrayColumnTypes();
                    columnTypes.add(ColumnType.LONG);
                    columnTypes.add(ColumnType.LONG);
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, columnTypes);
                    try {
                        return new Decimal16NthValueOverPartitionRowsFrameUnboundedFunction(map, partitionByRecord, partitionBySink, rowsHi, args.get(0), n, argType);
                    } catch (Throwable t) {
                        Misc.free(map);
                        throw t;
                    }
                } else {
                    ArrayColumnTypes columnTypes = new ArrayColumnTypes();
                    columnTypes.add(ColumnType.LONG);
                    columnTypes.add(ColumnType.LONG);
                    columnTypes.add(ColumnType.LONG);
                    Map map = MapFactory.createUnorderedMap(configuration, partitionByKeyTypes, columnTypes);
                    MemoryARW mem;
                    try {
                        mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                    } catch (Throwable t) {
                        Misc.free(map);
                        throw t;
                    }
                    try {
                        return new Decimal16NthValueOverPartitionRowsFrameFunction(map, partitionByRecord, partitionBySink,
                                rowsLo, rowsHi, args.get(0), mem, n, argType);
                    } catch (Throwable t) {
                        Misc.free(map);
                        Misc.free(mem);
                        throw t;
                    }
                }
            }
        } else {
            if (framingMode == WindowExpression.FRAMING_RANGE) {
                if (!windowContext.isOrdered() && windowContext.isDefaultFrame()) {
                    return new Decimal16NthValueOverWholeResultSetFunction(args.get(0), n, argType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    return new Decimal16NthValueOverWholeResultSetFunction(args.get(0), n, argType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    return new Decimal16NthValueOverUnboundedRowsFrameFunction(args.get(0), n, argType);
                } else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }
                    int timestampIndex = windowContext.getTimestampIndex();
                    return new Decimal16NthValueOverRangeFrameFunction(rowsLo, rowsHi, args.get(0), configuration, timestampIndex, n, argType);
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    return new Decimal16NthValueOverWholeResultSetFunction(args.get(0), n, argType);
                } else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    return new Decimal16NthValueOverUnboundedRowsFrameFunction(args.get(0), n, argType);
                } else if (rowsLo == 0 && rowsHi == 0) {
                    return new Decimal16NthValueOverCurrentRowFunction(args.get(0), n, argType);
                } else if (rowsLo == Long.MIN_VALUE) {
                    return new Decimal16NthValueOverRowsFrameUnboundedFunction(args.get(0), rowsHi, n, argType);
                } else {
                    MemoryARW mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                            configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
                    try {
                        return new Decimal16NthValueOverRowsFrameFunction(args.get(0), rowsLo, rowsHi, mem, n, argType);
                    } catch (Throwable t) {
                        Misc.free(mem);
                        throw t;
                    }
                }
            }
        }

        throw SqlException.$(position, "function not implemented for given window parameters");
    }

    static class Decimal16NthValueOverCurrentRowFunction extends BaseWindowFunction implements WindowDecimal16Function {

        private final int n;
        private final int type;
        private short value;

        Decimal16NthValueOverCurrentRowFunction(Function arg, int n, int type) {
            super(arg);
            this.n = n;
            this.type = type;
        }

        @Override
        public void computeNext(Record record) {
            value = n == 1 ? arg.getDecimal16(record) : Decimals.DECIMAL16_NULL;
        }

        @Override
        public short getDecimal16(Record rec) {
            return value;
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
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putShort(spi.getAddress(recordOffset, columnIndex), value);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(',').val(n).val(')');
            sink.val(" over (rows between current row and current row)");
        }
    }

    static class Decimal16NthValueOverPartitionFunction extends BasePartitionedWindowFunction implements WindowDecimal16Function {

        private final int n;
        private final int type;

        public Decimal16NthValueOverPartitionFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, int n, int type) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.n = n;
            this.type = type;
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public int getPassCount() {
            return WindowFunction.TWO_PASS;
        }

        @Override
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.createValue();

            if (value.isNew()) {
                if (n == 1) {
                    value.putLong(0, arg.getDecimal16(record));
                } else {
                    value.putLong(0, Decimals.DECIMAL16_NULL);
                }
                value.putLong(1, 1);
            } else {
                long count = value.getLong(1) + 1;
                value.putLong(1, count);
                if (count == n) {
                    value.putLong(0, arg.getDecimal16(record));
                }
            }
        }

        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.findValue();
            assert value != null;
            Unsafe.putShort(spi.getAddress(recordOffset, columnIndex), (short) value.getLong(0));
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(',').val(n).val(')');
            sink.val(" over (");
            sink.val("partition by ");
            sink.val(partitionByRecord.getFunctions());
            sink.val(')');
        }
    }

    public static class Decimal16NthValueOverPartitionRangeFrameFunction extends BasePartitionedWindowFunction implements WindowDecimal16Function {

        protected static final int RECORD_SIZE = Long.BYTES + Short.BYTES;
        protected final boolean frameIncludesCurrentValue;
        protected final boolean frameLoBounded;
        protected final LongList freeList = new LongList();
        protected final int initialBufferSize;
        protected final long maxDiff;
        protected final MemoryARW memory;
        protected final RingBufferDesc memoryDesc = new RingBufferDesc();
        protected final long minDiff;
        protected final int n;
        protected final int timestampIndex;
        protected final int type;
        protected short nthValue;

        public Decimal16NthValueOverPartitionRangeFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rangeLo,
                long rangeHi,
                Function arg,
                MemoryARW memory,
                int initialBufferSize,
                int timestampIdx,
                int n,
                int type
        ) {
            super(map, partitionByRecord, partitionBySink, arg);
            frameLoBounded = rangeLo != Long.MIN_VALUE;
            maxDiff = frameLoBounded ? Math.abs(rangeLo) : Math.abs(rangeHi);
            minDiff = Math.abs(rangeHi);
            this.memory = memory;
            this.initialBufferSize = initialBufferSize;
            this.timestampIndex = timestampIdx;
            this.n = n;
            this.frameIncludesCurrentValue = rangeHi == 0;
            this.type = type;
        }

        @Override
        public void close() {
            super.close();
            memory.close();
            freeList.clear();
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mapValue = key.createValue();

            long frameSize;
            long startOffset;
            long size;
            long capacity;
            long firstIdx;
            long timestamp = record.getTimestamp(timestampIndex);
            short s = arg.getDecimal16(record);

            if (mapValue.isNew()) {
                capacity = initialBufferSize;
                startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
                firstIdx = 0;
                memory.putLong(startOffset, timestamp);
                memory.putShort(startOffset + Long.BYTES, s);
                size = 1;
                if (frameIncludesCurrentValue) {
                    frameSize = 1;
                    nthValue = n == 1 ? s : Decimals.DECIMAL16_NULL;
                } else {
                    frameSize = 0;
                    nthValue = Decimals.DECIMAL16_NULL;
                }
            } else {
                frameSize = mapValue.getLong(0);
                startOffset = mapValue.getLong(1);
                size = mapValue.getLong(2);
                capacity = mapValue.getLong(3);
                firstIdx = mapValue.getLong(4);

                if (!frameLoBounded && frameSize >= n) {
                    long nthIdx = (firstIdx + n - 1) % capacity;
                    nthValue = memory.getShort(startOffset + nthIdx * RECORD_SIZE + Long.BYTES);
                    return;
                }

                long newFirstIdx = firstIdx;
                if (frameLoBounded) {
                    for (long i = 0, nn = size; i < nn; i++) {
                        long idx = (firstIdx + i) % capacity;
                        long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                        if (Math.abs(timestamp - ts) > maxDiff) {
                            if (frameSize > 0) {
                                frameSize--;
                            }
                            newFirstIdx = (idx + 1) % capacity;
                            size--;
                        } else {
                            break;
                        }
                    }
                }
                firstIdx = newFirstIdx;

                if (size == capacity) {
                    memoryDesc.reset(capacity, startOffset, size, firstIdx, freeList);
                    expandRingBuffer(memory, memoryDesc, RECORD_SIZE);
                    capacity = memoryDesc.capacity;
                    startOffset = memoryDesc.startOffset;
                    firstIdx = memoryDesc.firstIdx;
                }
                memory.putLong(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE, timestamp);
                memory.putShort(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE + Long.BYTES, s);
                size++;

                if (frameLoBounded) {
                    for (long i = frameSize; i < size; i++) {
                        long idx = (firstIdx + i) % capacity;
                        long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                        long diff = Math.abs(ts - timestamp);
                        if (diff <= maxDiff && diff >= minDiff) {
                            frameSize++;
                        } else {
                            break;
                        }
                    }
                } else {
                    for (long i = frameSize; i < size; i++) {
                        long idx = (firstIdx + i) % capacity;
                        long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                        if (Math.abs(timestamp - ts) >= minDiff) {
                            frameSize++;
                        } else {
                            break;
                        }
                    }
                }

                if (frameSize >= n) {
                    long nthIdx = (firstIdx + n - 1) % capacity;
                    nthValue = memory.getShort(startOffset + nthIdx * RECORD_SIZE + Long.BYTES);
                } else {
                    nthValue = Decimals.DECIMAL16_NULL;
                }
            }

            mapValue.putLong(0, frameSize);
            mapValue.putLong(1, startOffset);
            mapValue.putLong(2, size);
            mapValue.putLong(3, capacity);
            mapValue.putLong(4, firstIdx);
        }

        @Override
        public short getDecimal16(Record rec) {
            return nthValue;
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
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putShort(spi.getAddress(recordOffset, columnIndex), nthValue);
        }

        @Override
        public void reopen() {
            super.reopen();
            freeList.clear();
            nthValue = Decimals.DECIMAL16_NULL;
        }

        @Override
        public void reset() {
            super.reset();
            memory.close();
            freeList.clear();
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(',').val(n).val(')');
            sink.val(" over (");
            sink.val("partition by ");
            sink.val(partitionByRecord.getFunctions());
            sink.val(" range between ");
            if (frameLoBounded) {
                sink.val(maxDiff).val(" preceding");
            } else {
                sink.val("unbounded preceding");
            }
            sink.val(" and ");
            if (minDiff == 0) {
                sink.val("current row");
            } else {
                sink.val(minDiff).val(" preceding");
            }
            sink.val(')');
        }

        @Override
        public void toTop() {
            super.toTop();
            memory.truncate();
            freeList.clear();
        }
    }

    public static class Decimal16NthValueOverPartitionRowsFrameFunction extends BasePartitionedWindowFunction implements WindowDecimal16Function {

        protected final int bufferSize;
        protected final int excludeCount;
        protected final boolean frameIncludesCurrentValue;
        protected final int frameSize;
        protected final MemoryARW memory;
        protected final int n;
        protected final int type;
        protected short nthValue;

        public Decimal16NthValueOverPartitionRowsFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rowsLo,
                long rowsHi,
                Function arg,
                MemoryARW memory,
                int n,
                int type
        ) {
            super(map, partitionByRecord, partitionBySink, arg);
            assert rowsLo > Long.MIN_VALUE;
            frameSize = (int) (rowsHi - rowsLo + (rowsHi < 0 ? 1 : 0));
            bufferSize = (int) Math.abs(rowsLo);
            this.excludeCount = rowsHi < 0 ? (int) Math.abs(rowsHi) : 0;
            this.frameIncludesCurrentValue = rowsHi == 0;
            this.memory = memory;
            this.n = n;
            this.type = type;
        }

        @Override
        public void close() {
            super.close();
            memory.close();
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mapValue = key.createValue();

            short s = arg.getDecimal16(record);

            long loIdx;
            long startOffset;
            long count;

            if (mapValue.isNew()) {
                loIdx = 0;
                count = 0;
                startOffset = memory.appendAddressFor((long) bufferSize * Short.BYTES) - memory.getPageAddress(0);
                mapValue.putLong(1, startOffset);
                for (int i = 0; i < bufferSize; i++) {
                    memory.putShort(startOffset + (long) i * Short.BYTES, Decimals.DECIMAL16_NULL);
                }
            } else {
                loIdx = mapValue.getLong(0);
                startOffset = mapValue.getLong(1);
                count = mapValue.getLong(2);
            }

            long effectiveCount = Math.min(count, bufferSize);
            long currentFrameSize;
            if (frameIncludesCurrentValue) {
                currentFrameSize = Math.min(effectiveCount, frameSize);
            } else {
                currentFrameSize = Math.clamp(count + 1 - excludeCount, 0, frameSize);
            }

            if (currentFrameSize > 0 || (count == 0 && frameIncludesCurrentValue)) {
                long frameStartIdx = (loIdx + bufferSize - effectiveCount) % bufferSize;
                if (frameIncludesCurrentValue) {
                    if (n <= currentFrameSize) {
                        long nthIdx = (frameStartIdx + n - 1) % bufferSize;
                        nthValue = memory.getShort(startOffset + nthIdx * Short.BYTES);
                    } else if (n == currentFrameSize + 1) {
                        nthValue = s;
                    } else {
                        nthValue = Decimals.DECIMAL16_NULL;
                    }
                } else {
                    if (n <= currentFrameSize) {
                        long nthIdx = (frameStartIdx + n - 1) % bufferSize;
                        nthValue = memory.getShort(startOffset + nthIdx * Short.BYTES);
                    } else {
                        nthValue = Decimals.DECIMAL16_NULL;
                    }
                }
            } else {
                nthValue = Decimals.DECIMAL16_NULL;
            }

            count = Math.min(count + 1, bufferSize);
            mapValue.putLong(0, (loIdx + 1) % bufferSize);
            mapValue.putLong(2, count);
            memory.putShort(startOffset + loIdx * Short.BYTES, s);
        }

        @Override
        public short getDecimal16(Record rec) {
            return nthValue;
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
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putShort(spi.getAddress(recordOffset, columnIndex), nthValue);
        }

        @Override
        public void reopen() {
            super.reopen();
        }

        @Override
        public void reset() {
            super.reset();
            memory.close();
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(',').val(n).val(')');
            sink.val(" over (");
            sink.val("partition by ");
            sink.val(partitionByRecord.getFunctions());
            sink.val(" rows between ");
            sink.val(bufferSize);
            sink.val(" preceding and ");
            if (frameIncludesCurrentValue) {
                sink.val("current row");
            } else {
                sink.val(excludeCount).val(" preceding");
            }
            sink.val(')');
        }

        @Override
        public void toTop() {
            super.toTop();
            memory.truncate();
        }
    }

    public static class Decimal16NthValueOverPartitionRowsFrameUnboundedFunction extends BasePartitionedWindowFunction implements WindowDecimal16Function {

        protected final int bufferSize;
        protected final int n;
        protected final int type;
        protected short nthValue;

        public Decimal16NthValueOverPartitionRowsFrameUnboundedFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rowsHi,
                Function arg,
                int n,
                int type
        ) {
            super(map, partitionByRecord, partitionBySink, arg);
            assert rowsHi < 0 && rowsHi != Long.MIN_VALUE;
            this.bufferSize = (int) Math.abs(rowsHi);
            this.n = n;
            this.type = type;
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mapValue = key.createValue();

            short s = arg.getDecimal16(record);
            long count;
            if (mapValue.isNew()) {
                count = 0;
                mapValue.putLong(1, Decimals.DECIMAL16_NULL);
            } else {
                count = mapValue.getLong(0);
            }
            count++;
            if (count == n) {
                mapValue.putLong(1, s);
            }
            if (count >= (long) n + bufferSize) {
                nthValue = (short) mapValue.getLong(1);
            } else {
                nthValue = Decimals.DECIMAL16_NULL;
            }
            mapValue.putLong(0, count);
        }

        @Override
        public short getDecimal16(Record rec) {
            return nthValue;
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
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putShort(spi.getAddress(recordOffset, columnIndex), nthValue);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(',').val(n).val(')');
            sink.val(" over (");
            sink.val("partition by ");
            sink.val(partitionByRecord.getFunctions());
            sink.val(" rows between unbounded preceding and ");
            sink.val(bufferSize).val(" preceding");
            sink.val(')');
        }
    }

    public static class Decimal16NthValueOverRangeFrameFunction extends BaseWindowFunction implements Reopenable, WindowDecimal16Function {
        protected static final int RECORD_SIZE = Long.BYTES + Short.BYTES;
        protected final boolean frameIncludesCurrentValue;
        protected final boolean frameLoBounded;
        protected final LongList freeList = new LongList();
        protected final long initialCapacity;
        protected final long maxDiff;
        protected final MemoryARW memory;
        protected final RingBufferDesc memoryDesc = new RingBufferDesc();
        protected final long minDiff;
        protected final int n;
        protected final int timestampIndex;
        protected final int type;
        protected long capacity;
        protected long firstIdx;
        protected long frameSize;
        protected short nthValue;
        protected long size;
        protected long startOffset;

        public Decimal16NthValueOverRangeFrameFunction(
                long rangeLo,
                long rangeHi,
                Function arg,
                CairoConfiguration configuration,
                int timestampIdx,
                int n,
                int type
        ) {
            super(arg);
            frameLoBounded = rangeLo != Long.MIN_VALUE;
            maxDiff = frameLoBounded ? Math.abs(rangeLo) : Math.abs(rangeHi);
            minDiff = Math.abs(rangeHi);
            timestampIndex = timestampIdx;
            initialCapacity = configuration.getSqlWindowStorePageSize() / RECORD_SIZE;
            capacity = initialCapacity;
            MemoryARW mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                    configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER);
            try {
                startOffset = mem.appendAddressFor(capacity * RECORD_SIZE) - mem.getPageAddress(0);
            } catch (Throwable t) {
                Misc.free(mem);
                throw t;
            }
            memory = mem;
            firstIdx = 0;
            frameSize = 0;
            frameIncludesCurrentValue = rangeHi == 0;
            this.n = n;
            this.type = type;
        }

        @Override
        public void close() {
            super.close();
            memory.close();
        }

        @Override
        public void computeNext(Record record) {
            if (!frameLoBounded && frameSize >= n) {
                long nthIdx = (firstIdx + n - 1) % capacity;
                nthValue = memory.getShort(startOffset + nthIdx * RECORD_SIZE + Long.BYTES);
                return;
            }

            long timestamp = record.getTimestamp(timestampIndex);
            short s = arg.getDecimal16(record);
            long newFirstIdx = firstIdx;

            if (frameLoBounded) {
                for (long i = 0, nn = size; i < nn; i++) {
                    long idx = (firstIdx + i) % capacity;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    if (Math.abs(timestamp - ts) > maxDiff) {
                        if (frameSize > 0) {
                            frameSize--;
                        }
                        newFirstIdx = (idx + 1) % capacity;
                        size--;
                    } else {
                        break;
                    }
                }
            }
            firstIdx = newFirstIdx;

            if (size == capacity) {
                memoryDesc.reset(capacity, startOffset, size, firstIdx, freeList);
                expandRingBuffer(memory, memoryDesc, RECORD_SIZE);
                capacity = memoryDesc.capacity;
                startOffset = memoryDesc.startOffset;
                firstIdx = memoryDesc.firstIdx;
            }
            memory.putLong(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE, timestamp);
            memory.putShort(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE + Long.BYTES, s);
            size++;

            if (frameLoBounded) {
                for (long i = frameSize, nn = size; i < nn; i++) {
                    long idx = (firstIdx + i) % capacity;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    long diff = Math.abs(ts - timestamp);
                    if (diff <= maxDiff && diff >= minDiff) {
                        frameSize++;
                    } else {
                        break;
                    }
                }
            } else {
                for (long i = frameSize, nn = size; i < nn; i++) {
                    long idx = (firstIdx + i) % capacity;
                    long ts = memory.getLong(startOffset + idx * RECORD_SIZE);
                    if (Math.abs(timestamp - ts) >= minDiff) {
                        frameSize++;
                    } else {
                        break;
                    }
                }
            }

            if (frameSize >= n) {
                long nthIdx = (firstIdx + n - 1) % capacity;
                nthValue = memory.getShort(startOffset + nthIdx * RECORD_SIZE + Long.BYTES);
            } else {
                nthValue = Decimals.DECIMAL16_NULL;
            }
        }

        @Override
        public short getDecimal16(Record rec) {
            return nthValue;
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
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putShort(spi.getAddress(recordOffset, columnIndex), nthValue);
        }

        @Override
        public void reopen() {
            nthValue = Decimals.DECIMAL16_NULL;
            capacity = initialCapacity;
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            frameSize = 0;
            size = 0;
            freeList.clear();
        }

        @Override
        public void reset() {
            super.reset();
            memory.close();
            freeList.clear();
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(',').val(n).val(')');
            sink.val(" over (");
            sink.val("range between ");
            if (frameLoBounded) {
                sink.val(maxDiff).val(" preceding");
            } else {
                sink.val("unbounded preceding");
            }
            sink.val(" and ");
            if (minDiff == 0) {
                sink.val("current row");
            } else {
                sink.val(minDiff).val(" preceding");
            }
            sink.val(')');
        }

        @Override
        public void toTop() {
            super.toTop();
            nthValue = Decimals.DECIMAL16_NULL;
            capacity = initialCapacity;
            memory.truncate();
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            frameSize = 0;
            size = 0;
            freeList.clear();
        }
    }

    public static class Decimal16NthValueOverRowsFrameFunction extends BaseWindowFunction implements Reopenable, WindowDecimal16Function {
        protected final MemoryARW buffer;
        protected final int bufferSize;
        protected final int excludeCount;
        protected final boolean frameIncludesCurrentValue;
        protected final int frameSize;
        protected final int n;
        protected final int type;
        protected long count = 0;
        protected int loIdx = 0;
        protected short nthValue;

        public Decimal16NthValueOverRowsFrameFunction(Function arg, long rowsLo, long rowsHi, MemoryARW memory, int n, int type) {
            super(arg);
            assert rowsLo > Long.MIN_VALUE;
            frameSize = (int) (rowsHi - rowsLo + (rowsHi < 0 ? 1 : 0));
            bufferSize = (int) Math.abs(rowsLo);
            excludeCount = rowsHi < 0 ? (int) Math.abs(rowsHi) : 0;
            frameIncludesCurrentValue = rowsHi == 0;
            this.buffer = memory;
            this.n = n;
            this.type = type;
            initBuffer();
        }

        @Override
        public void close() {
            super.close();
            buffer.close();
        }

        @Override
        public void computeNext(Record record) {
            short s = arg.getDecimal16(record);

            long effectiveCount = Math.min(count, bufferSize);
            long currentFrameElements;
            if (frameIncludesCurrentValue) {
                currentFrameElements = Math.min(effectiveCount, frameSize);
            } else {
                currentFrameElements = Math.clamp(count + 1 - excludeCount, 0, frameSize);
            }

            if (currentFrameElements > 0 || (count == 0 && frameIncludesCurrentValue)) {
                long frameStartIdx = (loIdx + bufferSize - effectiveCount) % bufferSize;
                if (frameIncludesCurrentValue) {
                    if (n <= currentFrameElements) {
                        long nthIdx = (frameStartIdx + n - 1) % bufferSize;
                        nthValue = buffer.getShort(nthIdx * Short.BYTES);
                    } else if (n == currentFrameElements + 1) {
                        nthValue = s;
                    } else {
                        nthValue = Decimals.DECIMAL16_NULL;
                    }
                } else {
                    if (n <= currentFrameElements) {
                        long nthIdx = (frameStartIdx + n - 1) % bufferSize;
                        nthValue = buffer.getShort(nthIdx * Short.BYTES);
                    } else {
                        nthValue = Decimals.DECIMAL16_NULL;
                    }
                }
            } else {
                nthValue = Decimals.DECIMAL16_NULL;
            }

            count = Math.min(count + 1, bufferSize);
            buffer.putShort((long) loIdx * Short.BYTES, s);
            loIdx = (loIdx + 1) % bufferSize;
        }

        @Override
        public short getDecimal16(Record rec) {
            return nthValue;
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
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putShort(spi.getAddress(recordOffset, columnIndex), nthValue);
        }

        @Override
        public void reopen() {
            nthValue = Decimals.DECIMAL16_NULL;
            loIdx = 0;
            initBuffer();
            count = 0;
        }

        @Override
        public void reset() {
            super.reset();
            buffer.close();
            nthValue = Decimals.DECIMAL16_NULL;
            loIdx = 0;
            count = 0;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(',').val(n).val(')');
            sink.val(" over (");
            sink.val(" rows between ");
            sink.val(bufferSize);
            sink.val(" preceding and ");
            if (frameIncludesCurrentValue) {
                sink.val("current row");
            } else {
                sink.val(excludeCount).val(" preceding");
            }
            sink.val(')');
        }

        @Override
        public void toTop() {
            super.toTop();
            nthValue = Decimals.DECIMAL16_NULL;
            loIdx = 0;
            initBuffer();
            count = 0;
        }

        private void initBuffer() {
            for (int i = 0; i < bufferSize; i++) {
                buffer.putShort((long) i * Short.BYTES, Decimals.DECIMAL16_NULL);
            }
        }
    }

    public static class Decimal16NthValueOverRowsFrameUnboundedFunction extends BaseWindowFunction implements Reopenable, WindowDecimal16Function {
        protected final int bufferSize;
        protected final int n;
        protected final int type;
        protected short lockedValue = Decimals.DECIMAL16_NULL;
        protected short nthValue = Decimals.DECIMAL16_NULL;
        protected long totalCount = 0;

        public Decimal16NthValueOverRowsFrameUnboundedFunction(Function arg, long rowsHi, int n, int type) {
            super(arg);
            assert rowsHi < 0 && rowsHi != Long.MIN_VALUE;
            this.bufferSize = (int) Math.abs(rowsHi);
            this.n = n;
            this.type = type;
        }

        @Override
        public void computeNext(Record record) {
            short s = arg.getDecimal16(record);
            totalCount++;
            if (totalCount == n) {
                lockedValue = s;
            }
            nthValue = totalCount >= (long) n + bufferSize ? lockedValue : Decimals.DECIMAL16_NULL;
        }

        @Override
        public short getDecimal16(Record rec) {
            return nthValue;
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
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putShort(spi.getAddress(recordOffset, columnIndex), nthValue);
        }

        @Override
        public void reopen() {
            nthValue = Decimals.DECIMAL16_NULL;
            lockedValue = Decimals.DECIMAL16_NULL;
            totalCount = 0;
        }

        @Override
        public void reset() {
            super.reset();
            nthValue = Decimals.DECIMAL16_NULL;
            lockedValue = Decimals.DECIMAL16_NULL;
            totalCount = 0;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(',').val(n).val(')');
            sink.val(" over (");
            sink.val(" rows between unbounded preceding and ");
            sink.val(bufferSize).val(" preceding");
            sink.val(')');
        }

        @Override
        public void toTop() {
            super.toTop();
            nthValue = Decimals.DECIMAL16_NULL;
            lockedValue = Decimals.DECIMAL16_NULL;
            totalCount = 0;
        }
    }

    static class Decimal16NthValueOverUnboundedPartitionFrameFunction extends BasePartitionedWindowFunction implements WindowDecimal16Function {

        protected final boolean isRange;
        protected final int n;
        protected final int type;
        protected short value;

        public Decimal16NthValueOverUnboundedPartitionFrameFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, int n, boolean isRange, int type) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.n = n;
            this.isRange = isRange;
            this.type = type;
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mapValue = key.createValue();

            if (mapValue.isNew()) {
                if (n == 1) {
                    short s = arg.getDecimal16(record);
                    mapValue.putLong(0, s);
                    value = s;
                } else {
                    mapValue.putLong(0, Decimals.DECIMAL16_NULL);
                    value = Decimals.DECIMAL16_NULL;
                }
                mapValue.putLong(1, 1);
            } else {
                long count = mapValue.getLong(1);
                if (count >= n) {
                    value = (short) mapValue.getLong(0);
                } else {
                    count++;
                    mapValue.putLong(1, count);
                    if (count == n) {
                        short s = arg.getDecimal16(record);
                        mapValue.putLong(0, s);
                        value = s;
                    } else {
                        value = Decimals.DECIMAL16_NULL;
                    }
                }
            }
        }

        @Override
        public short getDecimal16(Record rec) {
            return value;
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
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putShort(spi.getAddress(recordOffset, columnIndex), value);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(',').val(n).val(')');
            sink.val(" over (");
            sink.val("partition by ");
            sink.val(partitionByRecord.getFunctions());
            sink.val(isRange ? " range" : " rows");
            sink.val(" between unbounded preceding and current row)");
        }
    }

    public static class Decimal16NthValueOverUnboundedRowsFrameFunction extends BaseWindowFunction implements WindowDecimal16Function {
        protected final int n;
        protected final int type;
        protected long count;
        protected boolean isFound;
        protected short value = Decimals.DECIMAL16_NULL;

        public Decimal16NthValueOverUnboundedRowsFrameFunction(Function arg, int n, int type) {
            super(arg);
            this.n = n;
            this.type = type;
        }

        @Override
        public void computeNext(Record record) {
            if (!isFound) {
                count++;
                if (count == n) {
                    value = arg.getDecimal16(record);
                    isFound = true;
                }
            }
        }

        @Override
        public short getDecimal16(Record rec) {
            return value;
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
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putShort(spi.getAddress(recordOffset, columnIndex), value);
        }

        @Override
        public void reset() {
            super.reset();
            count = 0;
            isFound = false;
            value = Decimals.DECIMAL16_NULL;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(',').val(n).val(')');
            sink.val(" over (rows between unbounded preceding and current row)");
        }

        @Override
        public void toTop() {
            super.toTop();
            count = 0;
            isFound = false;
            value = Decimals.DECIMAL16_NULL;
        }
    }

    public static class Decimal16NthValueOverWholeResultSetFunction extends BaseWindowFunction implements WindowDecimal16Function {
        protected final int n;
        protected final int type;
        protected long count;
        protected boolean isFound;
        protected short value = Decimals.DECIMAL16_NULL;

        public Decimal16NthValueOverWholeResultSetFunction(Function arg, int n, int type) {
            super(arg);
            this.n = n;
            this.type = type;
        }

        @Override
        public short getDecimal16(Record rec) {
            return value;
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public int getPassCount() {
            return WindowFunction.TWO_PASS;
        }

        @Override
        public int getType() {
            return type;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            if (!isFound) {
                count++;
                if (count == n) {
                    value = arg.getDecimal16(record);
                    isFound = true;
                }
            }
        }

        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            Unsafe.putShort(spi.getAddress(recordOffset, columnIndex), value);
        }

        @Override
        public void reset() {
            super.reset();
            clearState();
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(',').val(n).val(')');
            sink.val(" over ()");
        }

        @Override
        public void toTop() {
            super.toTop();
            clearState();
        }

        private void clearState() {
            count = 0;
            isFound = false;
            value = Decimals.DECIMAL16_NULL;
        }
    }

    static {
        NTH_VALUE_DECIMAL64_TYPES = new ArrayColumnTypes();
        NTH_VALUE_DECIMAL64_TYPES.add(ColumnType.LONG);
        NTH_VALUE_DECIMAL64_TYPES.add(ColumnType.LONG);
    }
}
