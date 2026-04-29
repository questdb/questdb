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
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;

/**
 * nth_value(expr, n) window function.
 * Returns the value of {@code expr} evaluated at the n-th row (1-based) of the window frame,
 * or NaN when {@code n} exceeds the current frame size.
 */
public class NthValueDoubleWindowFunctionFactory extends AbstractWindowFunctionFactory {

    public static final String NAME = "nth_value";
    private static final ArrayColumnTypes NTH_VALUE_COLUMN_TYPES;
    // LONG signature for n so both INT literals (auto-widened) and LONG literals resolve; the
    // value is validated to fit in a positive int below.
    private static final String SIGNATURE = NAME + "(DL)";

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
            return new DoubleNullFunction(
                    args.get(0),
                    NAME,
                    rowsLo,
                    rowsHi,
                    windowContext.getFramingMode() == WindowExpression.FRAMING_RANGE,
                    windowContext.getPartitionByRecord()
            );
        }
        // ROWS frame bounds map to int-sized ring-buffer indices; RANGE uses time deltas
        // (microseconds/nanoseconds) and only ever participates in long arithmetic.
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
                // whole partition (no order by, default frame) or (order by, unbounded preceding to unbounded following)
                if (windowContext.isDefaultFrame() && (!windowContext.isOrdered() || windowContext.getRowsHi() == Long.MAX_VALUE)) {
                    Map map = MapFactory.createUnorderedMap(
                            configuration,
                            partitionByKeyTypes,
                            NTH_VALUE_COLUMN_TYPES
                    );
                    try {
                        return new NthValueOverPartitionFunction(
                                map,
                                partitionByRecord,
                                partitionBySink,
                                args.get(0),
                                n
                        );
                    } catch (Throwable t) {
                        Misc.free(map);
                        throw t;
                    }
                } // between unbounded preceding and current row
                else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    Map map = MapFactory.createUnorderedMap(
                            configuration,
                            partitionByKeyTypes,
                            NTH_VALUE_COLUMN_TYPES
                    );
                    try {
                        return new NthValueOverUnboundedPartitionFrameFunction(
                                map,
                                partitionByRecord,
                                partitionBySink,
                                args.get(0),
                                n,
                                true
                        );
                    } catch (Throwable t) {
                        Misc.free(map);
                        throw t;
                    }
                } // range between [unbounded | x] preceding and [x preceding | current row]
                else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }

                    int timestampIndex = windowContext.getTimestampIndex();

                    ArrayColumnTypes columnTypes = new ArrayColumnTypes();
                    columnTypes.add(ColumnType.LONG);  // number of values in current frame
                    columnTypes.add(ColumnType.LONG);  // native array start offset, requires updating on resize
                    columnTypes.add(ColumnType.LONG);  // native buffer size
                    columnTypes.add(ColumnType.LONG);  // native buffer capacity
                    columnTypes.add(ColumnType.LONG);  // index of first buffered element

                    Map map = MapFactory.createUnorderedMap(
                            configuration,
                            partitionByKeyTypes,
                            columnTypes
                    );

                    final int initialBufferSize = configuration.getSqlWindowInitialRangeBufferSize();
                    MemoryARW mem;
                    try {
                        mem = Vm.getCARWInstance(
                                configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(),
                                MemoryTag.NATIVE_CIRCULAR_BUFFER
                        );
                    } catch (Throwable t) {
                        Misc.free(map);
                        throw t;
                    }

                    try {
                        return new NthValueOverPartitionRangeFrameFunction(
                                map,
                                partitionByRecord,
                                partitionBySink,
                                rowsLo,
                                rowsHi,
                                args.get(0),
                                mem,
                                initialBufferSize,
                                timestampIndex,
                                n
                        );
                    } catch (Throwable t) {
                        Misc.free(map);
                        Misc.free(mem);
                        throw t;
                    }
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                // between unbounded preceding and current row
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    Map map = MapFactory.createUnorderedMap(
                            configuration,
                            partitionByKeyTypes,
                            NTH_VALUE_COLUMN_TYPES
                    );
                    try {
                        return new NthValueOverUnboundedPartitionFrameFunction(
                                map,
                                partitionByRecord,
                                partitionBySink,
                                args.get(0),
                                n,
                                false
                        );
                    } catch (Throwable t) {
                        Misc.free(map);
                        throw t;
                    }
                } // between current row and current row
                else if (rowsLo == 0 && rowsHi == 0) {
                    return new NthValueOverCurrentRowFunction(args.get(0), n);
                } // whole partition
                else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    Map map = MapFactory.createUnorderedMap(
                            configuration,
                            partitionByKeyTypes,
                            NTH_VALUE_COLUMN_TYPES
                    );
                    try {
                        return new NthValueOverPartitionFunction(
                                map,
                                partitionByRecord,
                                partitionBySink,
                                args.get(0),
                                n
                        );
                    } catch (Throwable t) {
                        Misc.free(map);
                        throw t;
                    }
                }
                // unbounded preceding and K preceding (K > 0) -- no per-partition buffer needed.
                else if (rowsLo == Long.MIN_VALUE) {
                    ArrayColumnTypes columnTypes = new ArrayColumnTypes();
                    columnTypes.add(ColumnType.LONG); // count
                    columnTypes.add(ColumnType.LONG); // lockedValue (double bits as long)

                    Map map = MapFactory.createUnorderedMap(
                            configuration,
                            partitionByKeyTypes,
                            columnTypes
                    );
                    try {
                        return new NthValueOverPartitionRowsFrameUnboundedFunction(
                                map,
                                partitionByRecord,
                                partitionBySink,
                                rowsHi,
                                args.get(0),
                                n
                        );
                    } catch (Throwable t) {
                        Misc.free(map);
                        throw t;
                    }
                }
                // between X preceding and [Y preceding | current row]
                else {
                    ArrayColumnTypes columnTypes = new ArrayColumnTypes();
                    columnTypes.add(ColumnType.LONG); // position of current oldest element
                    columnTypes.add(ColumnType.LONG); // start offset of native array
                    columnTypes.add(ColumnType.LONG); // count of values in buffer

                    Map map = MapFactory.createUnorderedMap(
                            configuration,
                            partitionByKeyTypes,
                            columnTypes
                    );

                    MemoryARW mem;
                    try {
                        mem = Vm.getCARWInstance(
                                configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(),
                                MemoryTag.NATIVE_CIRCULAR_BUFFER
                        );
                    } catch (Throwable t) {
                        Misc.free(map);
                        throw t;
                    }

                    try {
                        return new NthValueOverPartitionRowsFrameFunction(
                                map,
                                partitionByRecord,
                                partitionBySink,
                                rowsLo,
                                rowsHi,
                                args.get(0),
                                mem,
                                n
                        );
                    } catch (Throwable t) {
                        Misc.free(map);
                        Misc.free(mem);
                        throw t;
                    }
                }
            }
        } else { // no partition key
            if (framingMode == WindowExpression.FRAMING_RANGE) {
                // if there's no order by then all elements are equal in range mode, thus calculation is done on whole result set
                if (!windowContext.isOrdered() && windowContext.isDefaultFrame()) {
                    return new NthValueOverWholeResultSetFunction(args.get(0), n);
                } // between unbounded preceding and unbounded following -- whole result set
                else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    return new NthValueOverWholeResultSetFunction(args.get(0), n);
                } // between unbounded preceding and current row
                else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    return new NthValueOverUnboundedRowsFrameFunction(args.get(0), n);
                } // range between [unbounded | x] preceding and [y preceding | current row]
                else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }

                    int timestampIndex = windowContext.getTimestampIndex();

                    return new NthValueOverRangeFrameFunction(
                            rowsLo,
                            rowsHi,
                            args.get(0),
                            configuration,
                            timestampIndex,
                            n
                    );
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                // between unbounded preceding and unbounded following -- whole result set
                if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    return new NthValueOverWholeResultSetFunction(args.get(0), n);
                } // between unbounded preceding and current row
                else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    return new NthValueOverUnboundedRowsFrameFunction(args.get(0), n);
                } // between current row and current row
                else if (rowsLo == 0 && rowsHi == 0) {
                    return new NthValueOverCurrentRowFunction(args.get(0), n);
                } // between unbounded preceding and K preceding (K > 0) -- no buffer needed.
                else if (rowsLo == Long.MIN_VALUE) {
                    return new NthValueOverRowsFrameUnboundedFunction(args.get(0), rowsHi, n);
                } // between X preceding and [Y preceding | current row]
                else {
                    MemoryARW mem = Vm.getCARWInstance(
                            configuration.getSqlWindowStorePageSize(),
                            configuration.getSqlWindowStoreMaxPages(),
                            MemoryTag.NATIVE_CIRCULAR_BUFFER
                    );

                    try {
                        return new NthValueOverRowsFrameFunction(
                                args.get(0),
                                rowsLo,
                                rowsHi,
                                mem,
                                n
                        );
                    } catch (Throwable t) {
                        Misc.free(mem);
                        throw t;
                    }
                }
            }
        }

        throw SqlException.$(position, "function not implemented for given window parameters");
    }

    // (rows between current row and current row) processes 1-element-big set
    // n=1 returns the current value, n>1 returns NaN
    static class NthValueOverCurrentRowFunction extends BaseWindowFunction implements WindowDoubleFunction {

        private final int n;
        private double value;

        NthValueOverCurrentRowFunction(Function arg, int n) {
            super(arg);
            this.n = n;
        }

        @Override
        public void computeNext(Record record) {
            value = n == 1 ? arg.getDouble(record) : Double.NaN;
        }

        @Override
        public double getDouble(Record rec) {
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
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), value);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(NAME);
            sink.val('(').val(arg).val(',').val(n).val(')');
            sink.val(" over (rows between current row and current row)");
        }
    }

    // handles nth_value() over (partition by x)
    // order by is absent so default frame mode includes all rows in the partition
    // TWO_PASS: pass1 counts rows per partition, stores nth value when found; pass2 emits
    static class NthValueOverPartitionFunction extends BasePartitionedWindowFunction implements WindowDoubleFunction {

        private final int n;

        public NthValueOverPartitionFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, int n) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.n = n;
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
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.createValue();

            if (value.isNew()) {
                if (n == 1) {
                    value.putDouble(0, arg.getDouble(record));
                } else {
                    value.putDouble(0, Double.NaN);
                }
                value.putLong(1, 1);
            } else {
                long count = value.getLong(1) + 1;
                value.putLong(1, count);
                if (count == n) {
                    value.putDouble(0, arg.getDouble(record));
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
            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), value.getDouble(0));
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(NAME);
            sink.val('(').val(arg).val(',').val(n).val(')');
            sink.val(" over (");
            sink.val("partition by ");
            sink.val(partitionByRecord.getFunctions());
            sink.val(')');
        }
    }

    // Handles nth_value() over (partition by x order by ts range between y preceding and [z preceding | current row])
    // Removable cumulative aggregation with timestamp & value stored in resizable ring buffers
    public static class NthValueOverPartitionRangeFrameFunction extends BasePartitionedWindowFunction implements WindowDoubleFunction {

        protected static final int RECORD_SIZE = Long.BYTES + Double.BYTES;
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
        protected double nthValue;

        public NthValueOverPartitionRangeFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rangeLo,
                long rangeHi,
                Function arg,
                MemoryARW memory,
                int initialBufferSize,
                int timestampIdx,
                int n
        ) {
            super(map, partitionByRecord, partitionBySink, arg);
            frameLoBounded = rangeLo != Long.MIN_VALUE;
            maxDiff = frameLoBounded ? Math.abs(rangeLo) : Math.abs(rangeHi);
            minDiff = Math.abs(rangeHi);
            this.memory = memory;
            this.initialBufferSize = initialBufferSize;
            this.timestampIndex = timestampIdx;
            this.n = n;
            frameIncludesCurrentValue = rangeHi == 0;
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
            double d = arg.getDouble(record);

            if (mapValue.isNew()) {
                capacity = initialBufferSize;
                startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
                firstIdx = 0;

                memory.putLong(startOffset, timestamp);
                memory.putDouble(startOffset + Long.BYTES, d);
                size = 1;

                if (frameIncludesCurrentValue) {
                    frameSize = 1;
                    nthValue = n == 1 ? d : Double.NaN;
                } else {
                    frameSize = 0;
                    nthValue = Double.NaN;
                }
            } else {
                frameSize = mapValue.getLong(0);
                startOffset = mapValue.getLong(1);
                size = mapValue.getLong(2);
                capacity = mapValue.getLong(3);
                firstIdx = mapValue.getLong(4);

                if (!frameLoBounded && frameSize >= n) {
                    // Once frameSize >= n on an unbounded-preceding frame the n-th value is locked;
                    // all map state (size, frameSize, firstIdx, capacity) stays frozen.
                    long nthIdx = (firstIdx + n - 1) % capacity;
                    nthValue = memory.getDouble(startOffset + nthIdx * RECORD_SIZE + Long.BYTES);
                    return;
                }

                long newFirstIdx = firstIdx;

                if (frameLoBounded) {
                    // find new bottom border of range frame and remove unneeded elements
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

                // add new element
                if (size == capacity) { // buffer full
                    memoryDesc.reset(capacity, startOffset, size, firstIdx, freeList);
                    expandRingBuffer(memory, memoryDesc, RECORD_SIZE);
                    capacity = memoryDesc.capacity;
                    startOffset = memoryDesc.startOffset;
                    firstIdx = memoryDesc.firstIdx;
                }

                // add element to buffer
                memory.putLong(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE, timestamp);
                memory.putDouble(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE + Long.BYTES, d);
                size++;

                // find new top border of range frame and add new elements
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
                    // unbounded preceding: firstIdx never retreats; extend frame with every
                    // previously-ineligible row whose timestamp is now at least minDiff behind.
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
                    nthValue = memory.getDouble(startOffset + nthIdx * RECORD_SIZE + Long.BYTES);
                } else {
                    nthValue = Double.NaN;
                }
            }

            mapValue.putLong(0, frameSize);
            mapValue.putLong(1, startOffset);
            mapValue.putLong(2, size);
            mapValue.putLong(3, capacity);
            mapValue.putLong(4, firstIdx);
        }

        @Override
        public double getDouble(Record rec) {
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
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), nthValue);
        }

        @Override
        public void reopen() {
            super.reopen();
            freeList.clear();
            nthValue = Double.NaN;
            // memory will allocate on first use via MemoryCARWImpl.appendAddressFor
        }

        @Override
        public void reset() {
            super.reset();
            // Releases native pages so cursor RSS budget after close stays within limits;
            // reopen() relies on MemoryCARWImpl.appendAddressFor lazily re-allocating from
            // pageAddress=0. Same pattern as FirstValue/LastValue/Avg sibling factories.
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

    // handles nth_value() over (partition by x [order by o] rows between K preceding and
    // [Y preceding | current row]). The unbounded-preceding..K-preceding variant uses a
    // simpler state-machine class (NthValueOverPartitionRowsFrameUnboundedFunction) that
    // does not allocate a per-partition buffer.
    // removable cumulative aggregation
    public static class NthValueOverPartitionRowsFrameFunction extends BasePartitionedWindowFunction implements WindowDoubleFunction {

        protected final int bufferSize;
        protected final int excludeCount;
        protected final boolean frameIncludesCurrentValue;
        protected final int frameSize;
        protected final MemoryARW memory;
        protected final int n;
        protected double nthValue;

        public NthValueOverPartitionRowsFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rowsLo,
                long rowsHi,
                Function arg,
                MemoryARW memory,
                int n
        ) {
            super(map, partitionByRecord, partitionBySink, arg);
            assert rowsLo > Long.MIN_VALUE; // use NthValueOverPartitionRowsFrameUnboundedFunction for the unbounded-lo case
            frameSize = (int) (rowsHi - rowsLo + (rowsHi < 0 ? 1 : 0));
            bufferSize = (int) Math.abs(rowsLo);
            this.excludeCount = rowsHi < 0 ? (int) Math.abs(rowsHi) : 0;
            this.frameIncludesCurrentValue = rowsHi == 0;
            this.memory = memory;
            this.n = n;
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

            double d = arg.getDouble(record);

            long loIdx;
            long startOffset;
            long count;

            if (mapValue.isNew()) {
                loIdx = 0;
                count = 0;
                startOffset = memory.appendAddressFor((long) bufferSize * Double.BYTES) - memory.getPageAddress(0);
                mapValue.putLong(1, startOffset);
                for (int i = 0; i < bufferSize; i++) {
                    memory.putDouble(startOffset + (long) i * Double.BYTES, Double.NaN);
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
                currentFrameSize = Math.max(0, Math.min(count + 1 - excludeCount, frameSize));
            }

            if (currentFrameSize > 0 || (count == 0 && frameIncludesCurrentValue)) {
                long frameStartIdx = (loIdx + bufferSize - effectiveCount) % bufferSize;
                if (frameIncludesCurrentValue) {
                    if (n <= currentFrameSize) {
                        long nthIdx = (frameStartIdx + n - 1) % bufferSize;
                        nthValue = memory.getDouble(startOffset + nthIdx * Double.BYTES);
                    } else if (n == currentFrameSize + 1) {
                        nthValue = d;
                    } else {
                        nthValue = Double.NaN;
                    }
                } else {
                    if (n <= currentFrameSize) {
                        long nthIdx = (frameStartIdx + n - 1) % bufferSize;
                        nthValue = memory.getDouble(startOffset + nthIdx * Double.BYTES);
                    } else {
                        nthValue = Double.NaN;
                    }
                }
            } else {
                nthValue = Double.NaN;
            }

            count = Math.min(count + 1, bufferSize);
            mapValue.putLong(0, (loIdx + 1) % bufferSize);
            mapValue.putLong(2, count);

            memory.putDouble(startOffset + loIdx * Double.BYTES, d);
        }

        @Override
        public double getDouble(Record rec) {
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
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), nthValue);
        }

        @Override
        public void reopen() {
            super.reopen();
            // memory will allocate on first use
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

    // Handles nth_value() over (partition by x [order by o] rows between unbounded preceding
    // and K preceding), K > 0. Once each partition has seen n rows, the n-th value is locked
    // and emitted whenever the frame first contains at least n rows (count >= n + bufferSize).
    // No per-partition buffer -- O(1) state per partition (count + lockedValue).
    public static class NthValueOverPartitionRowsFrameUnboundedFunction extends BasePartitionedWindowFunction implements WindowDoubleFunction {

        protected final int bufferSize;
        protected final int n;
        protected double nthValue;

        public NthValueOverPartitionRowsFrameUnboundedFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rowsHi,
                Function arg,
                int n
        ) {
            super(map, partitionByRecord, partitionBySink, arg);
            assert rowsHi < 0 && rowsHi != Long.MIN_VALUE; // K preceding with K > 0; (int) Math.abs would overflow at MIN_VALUE
            this.bufferSize = (int) Math.abs(rowsHi);
            this.n = n;
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mapValue = key.createValue();

            double d = arg.getDouble(record);
            long count;
            if (mapValue.isNew()) {
                count = 0;
                mapValue.putLong(1, Double.doubleToRawLongBits(Double.NaN));
            } else {
                count = mapValue.getLong(0);
            }
            count++;
            if (count == n) {
                mapValue.putLong(1, Double.doubleToRawLongBits(d));
            }
            if (count >= (long) n + bufferSize) {
                nthValue = Double.longBitsToDouble(mapValue.getLong(1));
            } else {
                nthValue = Double.NaN;
            }
            mapValue.putLong(0, count);
        }

        @Override
        public double getDouble(Record rec) {
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
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), nthValue);
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

    // Handles nth_value() over ([order by ts] range between x preceding and [ y preceding | current row ] ); no partition by key
    public static class NthValueOverRangeFrameFunction extends BaseWindowFunction implements Reopenable, WindowDoubleFunction {
        protected static final int RECORD_SIZE = Long.BYTES + Double.BYTES;
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
        protected long capacity;
        protected long firstIdx;
        protected long frameSize;
        protected double nthValue;
        protected long size;
        protected long startOffset;

        public NthValueOverRangeFrameFunction(
                long rangeLo,
                long rangeHi,
                Function arg,
                CairoConfiguration configuration,
                int timestampIdx,
                int n
        ) {
            super(arg);
            frameLoBounded = rangeLo != Long.MIN_VALUE;
            maxDiff = frameLoBounded ? Math.abs(rangeLo) : Math.abs(rangeHi);
            minDiff = Math.abs(rangeHi);
            timestampIndex = timestampIdx;
            initialCapacity = configuration.getSqlWindowStorePageSize() / RECORD_SIZE;

            capacity = initialCapacity;
            MemoryARW mem = Vm.getCARWInstance(
                    configuration.getSqlWindowStorePageSize(),
                    configuration.getSqlWindowStoreMaxPages(),
                    MemoryTag.NATIVE_CIRCULAR_BUFFER
            );
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
                nthValue = memory.getDouble(startOffset + nthIdx * RECORD_SIZE + Long.BYTES);
                return;
            }

            long timestamp = record.getTimestamp(timestampIndex);
            double d = arg.getDouble(record);

            long newFirstIdx = firstIdx;

            if (frameLoBounded) {
                // find new bottom border of range frame and remove unneeded elements
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

            // add new element
            if (size == capacity) { // buffer full
                memoryDesc.reset(capacity, startOffset, size, firstIdx, freeList);
                expandRingBuffer(memory, memoryDesc, RECORD_SIZE);
                capacity = memoryDesc.capacity;
                startOffset = memoryDesc.startOffset;
                firstIdx = memoryDesc.firstIdx;
            }

            // add element to buffer
            memory.putLong(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE, timestamp);
            memory.putDouble(startOffset + ((firstIdx + size) % capacity) * RECORD_SIZE + Long.BYTES, d);
            size++;

            // find new top border of range frame and add new elements
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
                // unbounded preceding: firstIdx never retreats; extend frame with every
                // previously-ineligible row whose timestamp is now at least minDiff behind.
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
                nthValue = memory.getDouble(startOffset + nthIdx * RECORD_SIZE + Long.BYTES);
            } else {
                nthValue = Double.NaN;
            }
        }

        @Override
        public double getDouble(Record rec) {
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
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), nthValue);
        }

        @Override
        public void reopen() {
            nthValue = Double.NaN;
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
            nthValue = Double.NaN;
            capacity = initialCapacity;
            memory.truncate();
            startOffset = memory.appendAddressFor(capacity * RECORD_SIZE) - memory.getPageAddress(0);
            firstIdx = 0;
            frameSize = 0;
            size = 0;
            freeList.clear();
        }
    }

    // Handles nth_value() over ([order by o] rows between K preceding and [Y preceding |
    // current row]); there's no partition by. The unbounded-preceding..K-preceding variant
    // uses a simpler state-machine class (NthValueOverRowsFrameUnboundedFunction) that does
    // not allocate a ring buffer.
    // Removable cumulative aggregation.
    public static class NthValueOverRowsFrameFunction extends BaseWindowFunction implements Reopenable, WindowDoubleFunction {
        protected final MemoryARW buffer;
        protected final int bufferSize;
        protected final int excludeCount;
        protected final boolean frameIncludesCurrentValue;
        protected final int frameSize;
        protected final int n;
        protected long count = 0;
        protected int loIdx = 0;
        protected double nthValue;

        public NthValueOverRowsFrameFunction(Function arg, long rowsLo, long rowsHi, MemoryARW memory, int n) {
            super(arg);
            // unbounded-lo cases route elsewhere: (MIN_VALUE, MAX_VALUE) -> NthValueOverWholeResultSetFunction,
            // (MIN_VALUE, 0) -> NthValueOverUnboundedRowsFrameFunction, (MIN_VALUE, K<0) -> NthValueOverRowsFrameUnboundedFunction
            assert rowsLo > Long.MIN_VALUE;
            frameSize = (int) (rowsHi - rowsLo + (rowsHi < 0 ? 1 : 0));
            bufferSize = (int) Math.abs(rowsLo);
            excludeCount = rowsHi < 0 ? (int) Math.abs(rowsHi) : 0;
            frameIncludesCurrentValue = rowsHi == 0;
            this.buffer = memory;
            this.n = n;
            initBuffer();
        }

        @Override
        public void close() {
            super.close();
            buffer.close();
        }

        @Override
        public void computeNext(Record record) {
            double d = arg.getDouble(record);

            long effectiveCount = Math.min(count, bufferSize);
            long currentFrameElements;
            if (frameIncludesCurrentValue) {
                currentFrameElements = Math.min(effectiveCount, frameSize);
            } else {
                currentFrameElements = Math.max(0, Math.min(count + 1 - excludeCount, frameSize));
            }

            if (currentFrameElements > 0 || (count == 0 && frameIncludesCurrentValue)) {
                long frameStartIdx = (loIdx + bufferSize - effectiveCount) % bufferSize;
                if (frameIncludesCurrentValue) {
                    if (n <= currentFrameElements) {
                        long nthIdx = (frameStartIdx + n - 1) % bufferSize;
                        nthValue = buffer.getDouble(nthIdx * Double.BYTES);
                    } else if (n == currentFrameElements + 1) {
                        nthValue = d;
                    } else {
                        nthValue = Double.NaN;
                    }
                } else {
                    if (n <= currentFrameElements) {
                        long nthIdx = (frameStartIdx + n - 1) % bufferSize;
                        nthValue = buffer.getDouble(nthIdx * Double.BYTES);
                    } else {
                        nthValue = Double.NaN;
                    }
                }
            } else {
                nthValue = Double.NaN;
            }

            count = Math.min(count + 1, bufferSize);
            buffer.putDouble((long) loIdx * Double.BYTES, d);
            loIdx = (loIdx + 1) % bufferSize;
        }

        @Override
        public double getDouble(Record rec) {
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
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), nthValue);
        }

        @Override
        public void reopen() {
            nthValue = Double.NaN;
            loIdx = 0;
            initBuffer();
            count = 0;
        }

        @Override
        public void reset() {
            super.reset();
            buffer.close();
            nthValue = Double.NaN;
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
            nthValue = Double.NaN;
            loIdx = 0;
            initBuffer();
            count = 0;
        }

        private void initBuffer() {
            for (int i = 0; i < bufferSize; i++) {
                buffer.putDouble((long) i * Double.BYTES, Double.NaN);
            }
        }
    }

    // Handles nth_value() over ([order by o] rows between unbounded preceding and K preceding),
    // K > 0, with no partition by. Once totalCount reaches n, the n-th value is captured in
    // lockedValue and emitted whenever the frame first contains at least n rows. No buffer.
    public static class NthValueOverRowsFrameUnboundedFunction extends BaseWindowFunction implements Reopenable, WindowDoubleFunction {
        protected final int bufferSize;
        protected final int n;
        protected double lockedValue = Double.NaN;
        protected double nthValue = Double.NaN;
        protected long totalCount = 0;

        public NthValueOverRowsFrameUnboundedFunction(Function arg, long rowsHi, int n) {
            super(arg);
            assert rowsHi < 0 && rowsHi != Long.MIN_VALUE; // K preceding with K > 0; (int) Math.abs would overflow at MIN_VALUE
            this.bufferSize = (int) Math.abs(rowsHi);
            this.n = n;
        }

        @Override
        public void computeNext(Record record) {
            double d = arg.getDouble(record);
            totalCount++;
            if (totalCount == n) {
                lockedValue = d;
            }
            nthValue = totalCount >= (long) n + bufferSize ? lockedValue : Double.NaN;
        }

        @Override
        public double getDouble(Record rec) {
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
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), nthValue);
        }

        @Override
        public void reopen() {
            nthValue = Double.NaN;
            lockedValue = Double.NaN;
            totalCount = 0;
        }

        @Override
        public void reset() {
            super.reset();
            nthValue = Double.NaN;
            lockedValue = Double.NaN;
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
            nthValue = Double.NaN;
            lockedValue = Double.NaN;
            totalCount = 0;
        }
    }

    // Handles:
    // - nth_value(a, n) over (partition by x rows between unbounded preceding and current row)
    // - nth_value(a, n) over (partition by x order by ts range between unbounded preceding and current row)
    // RANGE follows the project-wide QuestDB convention (shared with sum/avg/min/max/first_value/
    // last_value): a frame ending at CURRENT ROW does not look ahead to peer rows that share the
    // same ORDER BY value. The isRange flag only affects EXPLAIN output. This diverges from the
    // SQL standard (Postgres) on tied ORDER BY values; revisiting peer semantics is tracked as a
    // project-wide follow-up that should cover all 20+ RANGE-supporting window factories together.
    static class NthValueOverUnboundedPartitionFrameFunction extends BasePartitionedWindowFunction implements WindowDoubleFunction {

        protected final boolean isRange;
        protected final int n;
        protected double value;

        public NthValueOverUnboundedPartitionFrameFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, int n, boolean isRange) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.n = n;
            this.isRange = isRange;
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mapValue = key.createValue();

            if (mapValue.isNew()) {
                if (n == 1) {
                    double d = arg.getDouble(record);
                    mapValue.putDouble(0, d);
                    value = d;
                } else {
                    mapValue.putDouble(0, Double.NaN);
                    value = Double.NaN;
                }
                mapValue.putLong(1, 1);
            } else {
                long count = mapValue.getLong(1);
                if (count >= n) {
                    // nth value already found
                    value = mapValue.getDouble(0);
                } else {
                    count++;
                    mapValue.putLong(1, count);
                    if (count == n) {
                        double d = arg.getDouble(record);
                        mapValue.putDouble(0, d);
                        value = d;
                    } else {
                        value = Double.NaN;
                    }
                }
            }
        }

        @Override
        public double getDouble(Record rec) {
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
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), value);
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

    // handles nth_value() over () or nth_value() over (rows between unbounded preceding and current row); no partition by
    // Counts rows; when count reaches n, stores the value. Emits stored value on all subsequent rows.
    public static class NthValueOverUnboundedRowsFrameFunction extends BaseWindowFunction implements WindowDoubleFunction {
        protected final int n;
        protected long count;
        protected boolean isFound;
        protected double value = Double.NaN;

        public NthValueOverUnboundedRowsFrameFunction(Function arg, int n) {
            super(arg);
            this.n = n;
        }

        @Override
        public void computeNext(Record record) {
            if (!isFound) {
                count++;
                if (count == n) {
                    value = arg.getDouble(record);
                    isFound = true;
                }
            }
        }

        @Override
        public double getDouble(Record rec) {
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
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), value);
        }

        @Override
        public void reset() {
            super.reset();
            count = 0;
            isFound = false;
            value = Double.NaN;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(NAME);
            sink.val('(').val(arg).val(',').val(n).val(')');
            sink.val(" over (rows between unbounded preceding and current row)");
        }

        @Override
        public void toTop() {
            super.toTop();
            count = 0;
            isFound = false;
            value = Double.NaN;
        }
    }

    // handles nth_value() over () with default frame and no order by
    // all rows are in one partition; TWO_PASS: pass1 finds nth value, pass2 writes it
    public static class NthValueOverWholeResultSetFunction extends BaseWindowFunction implements WindowDoubleFunction {
        protected final int n;
        protected long count;
        protected boolean isFound;
        protected double value = Double.NaN;

        public NthValueOverWholeResultSetFunction(Function arg, int n) {
            super(arg);
            this.n = n;
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
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            if (!isFound) {
                count++;
                if (count == n) {
                    value = arg.getDouble(record);
                    isFound = true;
                }
            }
        }

        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), value);
        }

        @Override
        public void reset() {
            super.reset();
            count = 0;
            isFound = false;
            value = Double.NaN;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(NAME);
            sink.val('(').val(arg).val(',').val(n).val(')');
            sink.val(" over ()");
        }

        @Override
        public void toTop() {
            super.toTop();
            count = 0;
            isFound = false;
            value = Double.NaN;
        }
    }

    static {
        NTH_VALUE_COLUMN_TYPES = new ArrayColumnTypes();
        NTH_VALUE_COLUMN_TYPES.add(ColumnType.DOUBLE);
        NTH_VALUE_COLUMN_TYPES.add(ColumnType.LONG); // row count within partition
    }
}
