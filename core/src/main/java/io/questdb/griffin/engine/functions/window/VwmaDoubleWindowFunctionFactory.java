package io.questdb.griffin.engine.functions.window;

import org.jetbrains.annotations.NotNull;

import io.questdb.cairo.*;
import io.questdb.cairo.map.*;
import io.questdb.cairo.sql.*;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.vm.api.MemoryARW;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.window.WindowContext;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.griffin.model.WindowColumn;
import io.questdb.std.*;

public class VwmaDoubleWindowFunctionFactory implements FunctionFactory {

    public static final ArrayColumnTypes VWMA_COLUMN_TYPES;
    public static final ArrayColumnTypes VWMA_OVER_PARTITION_ROWS_COLUMN_TYPES;
    private static final String NAME = "vwma";

    @Override
    public String getSignature() {
        return NAME + "(DD)";
    }

    @Override
    public boolean isWindow() {
        return true;
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

        long rowsLo = windowContext.getRowsLo();
        long rowsHi = windowContext.getRowsHi();

        if (!windowContext.isDefaultFrame()) {
            if (rowsLo > 0) {
                throw SqlException.$(windowContext.getRowsLoKindPos(), "frame start supports UNBOUNDED PRECEDING, _number_ PRECEDING and CURRENT ROW only");
            }
            if (rowsHi > 0) {
                if (rowsHi != Long.MAX_VALUE) {
                    throw SqlException.$(windowContext.getRowsHiKindPos(), "frame end supports _number_ PRECEDING and CURRENT ROW only");
                } else if (rowsLo != Long.MIN_VALUE) {
                    throw SqlException.$(windowContext.getRowsHiKindPos(), "frame end supports UNBOUNDED FOLLOWING only when frame start is UNBOUNDED PRECEDING");
                }
            }
        }

        int exclusionKind = windowContext.getExclusionKind();
        int exclusionKindPos = windowContext.getExclusionKindPos();
        if (exclusionKind != WindowColumn.EXCLUDE_NO_OTHERS
                && exclusionKind != WindowColumn.EXCLUDE_CURRENT_ROW) {
            throw SqlException.$(exclusionKindPos, "only EXCLUDE NO OTHERS and EXCLUDE CURRENT ROW exclusion modes are supported");
        }

        if (exclusionKind == WindowColumn.EXCLUDE_CURRENT_ROW) {
            // assumes frame doesn't use 'following'
            if (rowsHi == Long.MAX_VALUE) {
                throw SqlException.$(exclusionKindPos, "EXCLUDE CURRENT ROW not supported with UNBOUNDED FOLLOWING frame boundary");
            }

            if (rowsHi == 0) {
                rowsHi = -1;
            }
            if (rowsHi < rowsLo) {
                throw SqlException.$(exclusionKindPos, "end of window is higher than start of window due to exclusion mode");
            }
        }

        int framingMode = windowContext.getFramingMode();
        if (framingMode == WindowColumn.FRAMING_GROUPS) {
            throw SqlException.$(position, "function not implemented for given window parameters");
        }

        RecordSink partitionBySink = windowContext.getPartitionBySink();
        ColumnTypes partitionByKeyTypes = windowContext.getPartitionByKeyTypes();
        VirtualRecord partitionByRecord = windowContext.getPartitionByRecord();

        if (partitionByRecord != null) {
            
            if (framingMode == WindowColumn.FRAMING_ROWS) {
            // between unbounded preceding and current row
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    Map map = MapFactory.createOrderedMap(
                            configuration,
                            partitionByKeyTypes,
                            VWMA_COLUMN_TYPES
                    );

                    return new VwmaOverUnboundedPartitionRowsFrameFunction(
                            map,
                            partitionByRecord,
                            partitionBySink,
                            args.get(0),
                            args.get(1)
                    );
                } // between current row and current row
                else if (rowsLo == 0 && rowsLo == rowsHi) {
                    return new VwmaOverCurrentRowFunction(args.get(0), args.get(1));
                } // whole partition
                else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    Map map = MapFactory.createOrderedMap(
                            configuration,
                            partitionByKeyTypes,
                            VWMA_COLUMN_TYPES
                    );

                    return new VwmaOverPartitionFunction(
                            map,
                            partitionByRecord,
                            partitionBySink,
                            args.get(0),
                            args.get(1)
                    );
                }
                //between [unbounded | x] preceding and [x preceding | current row]
                else {
                    ArrayColumnTypes columnTypes = new ArrayColumnTypes();
                    columnTypes.add(ColumnType.DOUBLE); // sum
                    columnTypes.add(ColumnType.LONG); // current frame size
                    columnTypes.add(ColumnType.LONG); // position of current oldest element
                    columnTypes.add(ColumnType.LONG); // start offset of native array

                    Map map = null;
                    MemoryARW mem = null;
                    try {
                        map = MapFactory.createOrderedMap(
                                configuration,
                                partitionByKeyTypes,
                                VWMA_COLUMN_TYPES
                        );
                        mem = Vm.getARWInstance(
                                configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(),
                                MemoryTag.NATIVE_CIRCULAR_BUFFER
                        );

                        // moving average over preceding N rows
                        return new VwmaOverPartitionRowsFrameFunction(
                                map,
                                partitionByRecord,
                                partitionBySink,
                                rowsLo,
                                rowsHi,
                                args.get(0),
                                args.get(1),
                                mem
                        );
                    } catch (Throwable th) {
                        Misc.free(map);
                        Misc.free(mem);
                        throw th;
                    }
                }
            }
        }
        else{
            // between unbounded preceding and current row

            if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                return new VwmaOverUnboundedRowsFrameFunction(args.get(0), args.get(1));
            } // between current row and current row
            else if (rowsLo == 0 && rowsLo == rowsHi) {
                return new VwmaOverCurrentRowFunction(args.get(0), args.get(1));
            } // whole result set
            else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                return new VwmaOverWholeResultSetFunction(args.get(0), args.get(1));
            } // between [unbounded | x] preceding and [x preceding | current row]
            else {
                MemoryARW mem = Vm.getARWInstance(
                        configuration.getSqlWindowStorePageSize(),
                        configuration.getSqlWindowStoreMaxPages(),
                        MemoryTag.NATIVE_CIRCULAR_BUFFER
                );
                return new VwmaOverRowsFrameFunction(
                        args.get(0),
                        args.get(1),
                        rowsLo,
                        rowsHi,
                        mem
                );
            }        
        }
        throw SqlException.$(position, "function not implemented for given window parameters");
    }

    // Handles:
    // - vwma(a) over (partition by x rows between unbounded preceding and current row)
    // - vwma(a) over (partition by x order by ts range between unbounded preceding and current row)
    // Doesn't require value buffering.
    static class VwmaOverUnboundedPartitionRowsFrameFunction extends TwoColumnPartitionedDoubleWindowFunction {
        private double vwma;

        public VwmaOverUnboundedPartitionRowsFrameFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg1, Function arg2) {
            super(map, partitionByRecord, partitionBySink, arg1, arg2);
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.createValue();

            double notional;
            double volumeSum;

            if (value.isNew()) {
                notional = 0;
                volumeSum = 0;
            } else {
                notional = value.getDouble(0);
                volumeSum = value.getDouble(1);
            }

            double price = arg1.getDouble(record);
            double volume = arg2.getDouble(record);

            if (Numbers.isFinite(price) && Numbers.isFinite(volume) && volume > 0.0) {
                notional += price * volume;
                volumeSum += volume;

                value.putDouble(0, notional);
                value.putDouble(1, volumeSum);
            }

            vwma = volumeSum != 0 ? notional / volumeSum : Double.NaN;
        }

        @Override
        public double getDouble(Record rec) {
            return vwma;
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
            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), vwma);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(NAME);
            sink.val('(').val(arg1).val(')');
            sink.val('(').val(arg2).val(')');
            sink.val(" over (");
            sink.val("partition by ");
            sink.val(partitionByRecord.getFunctions());
            sink.val(" rows between unbounded preceding and current row )");
        }
    }

    // handles vwma() over (partition by x)
    // order by is absent so default frame mode includes all rows in partition
    static class VwmaOverPartitionFunction extends TwoColumnPartitionedDoubleWindowFunction {

        public VwmaOverPartitionFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg1, Function arg2) {
            super(map, partitionByRecord, partitionBySink, arg1, arg2);
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
            double price = arg1.getDouble(record);
            double volume = arg2.getDouble(record);

            if (Numbers.isFinite(price) && Numbers.isFinite(volume) && volume > 0.0) {
                partitionByRecord.of(record);
                MapKey key = map.withKey();
                key.put(partitionByRecord, partitionBySink);
                MapValue value = key.createValue();

                double volumeSum;
                double notional;

                if (value.isNew()) {
                    volumeSum = volume;
                    notional = price * volume;
                } else {
                    volumeSum = value.getDouble(1) + volume;
                    notional = value.getDouble(0) + price * volume;
                }
                value.putDouble(0, notional);
                value.putDouble(1, volumeSum);
            }
        }

        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.findValue();

            double val = value != null ? value.getDouble(0) : Double.NaN;

            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), val);
        }

        @Override
        public void preparePass2() {
            RecordCursor cursor = map.getCursor();
            MapRecord record = map.getRecord();
            while (cursor.hasNext()) {
                MapValue value = record.getValue();
                double volumeSum = value.getDouble(1);
                if (volumeSum > 0) {
                    double notional = value.getDouble(0);
                    value.putDouble(0, notional / volumeSum);
                }
            }
        }
    }

    // handles vwma() over (partition by x [order by o] rows between y and z)
    // removable cumulative aggregation
    static class VwmaOverPartitionRowsFrameFunction extends TwoColumnPartitionedDoubleWindowFunction {

        //number of values we need to keep to compute over frame
        // (can be bigger than frame because we've to buffer values between rowsHi and current row )
        private final int bufferSize;

        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final int frameSize;
        // holds fixed-size ring buffers of double values
        private final MemoryARW memory;
        private double vwma;
        private double notional;
        private double volumeSum;


        public VwmaOverPartitionRowsFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rowsLo,
                long rowsHi,
                Function arg1,
                Function arg2,
                MemoryARW memory
        ) {
            super(map, partitionByRecord, partitionBySink, arg1, arg2);

            if (rowsLo > Long.MIN_VALUE) {
                frameSize = (int) (rowsHi - rowsLo + (rowsHi < 0 ? 1 : 0));
                bufferSize = (int) Math.abs(rowsLo);
                frameLoBounded = true;
            } else {
                frameSize = 1;
                bufferSize = (int) Math.abs(rowsHi);
                frameLoBounded = false;
            }
            frameIncludesCurrentValue = rowsHi == 0;

            this.memory = memory;
        }

        @Override
        public void close() {
            super.close();
            memory.close();
        }

        @Override
        public void computeNext(Record record) {
            // map stores:
            // 0 - sum, never store NaN in it
            // 1 - current number of non-null rows in frame
            // 2 - (0-based) index of oldest value [0, bufferSize]
            // 3 - native array start offset (relative to memory address)
            // we keep nulls in window and reject them when computing vwma

            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.createValue();

            long loIdx;//current index of lo frame value ('oldest')
            long startOffset;
            double price = arg1.getDouble(record);
            double volume = arg2.getDouble(record);

            if (value.isNew()) {

                loIdx = 0;
                startOffset = memory.appendAddressFor((long) bufferSize * Double.BYTES) - memory.getPageAddress(0);
                if (frameIncludesCurrentValue && Numbers.isFinite(price) && Numbers.isFinite(volume) && volume > 0.0) {
                    notional = price * volume;
                    volumeSum = volume;
                    vwma = price;
                } else {
                    notional = 0.0;
                    vwma = Double.NaN;
                    volumeSum = 0.0;
                }

                for (int i = 0; i < bufferSize; i++) {
                    memory.putDouble(startOffset + (long) i * Double.BYTES, Double.NaN);
                }
            } else {
                notional = value.getDouble(0);
                volumeSum = value.getDouble(1);
                loIdx = value.getLong(2);
                startOffset = value.getLong(3);

                //compute value using top frame element (that could be current or previous row)
                double hiValuePrice = frameIncludesCurrentValue ? price : memory.getDouble(startOffset + ((loIdx + frameSize - 1) % bufferSize) * Double.BYTES);
                double hiValueVolume = frameIncludesCurrentValue ? volume : memory.getDouble(startOffset + ((loIdx + frameSize - 1) % bufferSize) * Double.BYTES);

                if (Numbers.isFinite(hiValuePrice) && Numbers.isFinite(hiValueVolume) && hiValueVolume > 0.0) {
                    volumeSum += hiValueVolume;
                    notional += hiValuePrice * hiValueVolume;
                }

                //here sum is correct for current row
                if (volumeSum != 0) {
                    vwma = notional / volumeSum;
                } else {
                    vwma = Double.NaN;
                }


                if (frameLoBounded) {
                    //remove the oldest element
                    double loValuePrice = memory.getDouble(startOffset + loIdx * Double.BYTES);
                    double loValueVolume = memory.getDouble(startOffset + (loIdx + 1) * Double.BYTES);

                    if (Numbers.isFinite(loValuePrice) && Numbers.isFinite(loValueVolume) && loValueVolume > 0.0){
                        notional -= (loValuePrice * loValueVolume);
                        volumeSum -= loValueVolume;
                    }
                }
            }

            value.putDouble(0, notional);
            value.putDouble(1, volumeSum);
            value.putLong(2, (loIdx + 1) % bufferSize);
            value.putLong(3, startOffset);//not necessary because it doesn't change
            memory.putDouble(startOffset + loIdx * Double.BYTES, price);
            memory.putDouble(startOffset + (loIdx + 1) * Double.BYTES, volume);
        }

        @Override
        public double getDouble(Record rec) {
            return vwma;
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
            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), vwma);
        }

        @Override
        public void reopen() {
            super.reopen();
            // memory will allocate on first use
        }

        @Override
        public void reset() {
            super.reset();
            vwma = Double.NaN;
            notional = 0.0;
            volumeSum = 0.0;
            memory.close();
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg1).val(')');
            sink.val('(').val(arg2).val(')');
            sink.val(" over (");
            sink.val("partition by ");
            sink.val(partitionByRecord.getFunctions());

            sink.val(" rows between ");
            if (frameLoBounded) {
                sink.val(bufferSize);
            } else {
                sink.val("unbounded");
            }
            sink.val(" preceding and ");
            if (frameIncludesCurrentValue) {
                sink.val("current row");
            } else {
                sink.val(bufferSize - frameSize).val(" preceding");
            }
            sink.val(')');
        }

        @Override
        public void toTop() {
            super.toTop();
            memory.truncate();
        }
    }

    // Handles vwma() over (rows between unbounded preceding and current row); there's no partition by.
    static class VwmaOverUnboundedRowsFrameFunction extends TwoColumnDoubleWindowFunction {

        private double vwma = 0;
        private long count = 0;
        private double volumeSum = 0.0;
        private double notional = 0.0;

        public VwmaOverUnboundedRowsFrameFunction(Function arg1, Function arg2) {
            super(arg1, arg2);
        }

        @Override
        public void computeNext(Record record) {
            double price = arg1.getDouble(record);
            double volume = arg2.getDouble(record);            
            if (Numbers.isFinite(price) && Numbers.isFinite(volume) && volume > 0.0) {
                notional += price * volume;
                volumeSum += volume;
                count++;
            }

            vwma = count != 0 ? notional / volumeSum : Double.NaN;
        }

        @Override
        public double getDouble(Record rec) {
            return vwma;
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

            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), vwma);
        }

        @Override
        public void reset() {
            super.reset();
            vwma = Double.NaN;
            count = 0;
            notional = 0.0;
            volumeSum = 0.0;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(NAME);
            sink.val('(').val(arg1).val(')');
            sink.val('(').val(arg2).val(')');
            sink.val(" over (rows between unbounded preceding and current row)");
        }

        @Override
        public void toTop() {
            super.toTop();
            vwma = Double.NaN;
            count = 0;
            volumeSum = 0.0;
            notional = 0.0;
        }
    }    

    // vwma() over () - empty clause, no partition by no order by, no frame == default frame
    static class VwmaOverWholeResultSetFunction extends TwoColumnDoubleWindowFunction {
        private double vwma = 0;
        private long count = 0;
        private double volumeSum = 0.0;
        private double notional = 0.0;

        public VwmaOverWholeResultSetFunction(Function arg1, Function arg2) {
            super(arg1, arg2);
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
            double price = arg1.getDouble(record);
            double volume = arg2.getDouble(record);

            if (Numbers.isFinite(price) && Numbers.isFinite(volume) && volume > 0.0) {
                volumeSum += volume;
                notional += price * volume;
                count++;
            }
        }

        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), vwma);
        }

        @Override
        public void preparePass2() {
            vwma = count > 0 ? notional / volumeSum : Double.NaN;
        }

        @Override
        public void reset() {
            super.reset();
            vwma = Double.NaN;
            count = 0;
            notional = 0.0;
            volumeSum = 0.0;
        }

        @Override
        public void toTop() {
            super.toTop();
            vwma = Double.NaN;
            count = 0;
            notional = 0.0;
            volumeSum = 0.0;
        }

    }


    // (rows between current row and current row) processes 1-element-big set, so simply it returns expression value
    static class VwmaOverCurrentRowFunction extends TwoColumnDoubleWindowFunction {

        private double vwma = 0;

        VwmaOverCurrentRowFunction(Function arg1, Function arg2) {
            super(arg1, arg2);
        }

        @Override
        public void computeNext(Record record) {
            vwma = arg1.getDouble(record) / arg2.getDouble(record);

        }

        @Override
        public double getDouble(Record rec) {
            return vwma;
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public int getPassCount() {
            return ZERO_PASS;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), vwma);
        }
    }
    
    static class VwmaOverRowsFrameFunction extends TwoColumnDoubleWindowFunction implements Reopenable {
        private final MemoryARW buffer;
        private final int bufferSize;
        private final boolean frameIncludesCurrentValue;
        private final boolean frameLoBounded;
        private final int frameSize;
        protected double externalNotional = 0;
        private double vwma = 0;
        private long count = 0;
        private int loIdx = 0;
        private double volumeSum = 0.0;
        private double notional = 0.0;

        public VwmaOverRowsFrameFunction(@NotNull Function arg0, @NotNull Function arg1, long rowsLo, long rowsHi, MemoryARW memory) {
            super(arg0, arg1);
            
            assert rowsLo != Long.MIN_VALUE || rowsHi != 0; // use VwmaOverUnboundedRowsFrameFunction in case of (Long.MIN_VALUE, 0) range
            if (rowsLo > Long.MIN_VALUE) {
                frameSize = (int) (rowsHi - rowsLo + (rowsHi < 0 ? 1 : 0));
                bufferSize = (int) Math.abs(rowsLo);//number of values we need to keep to compute over frame
                frameLoBounded = true;
            } else {
                frameSize = (int) Math.abs(rowsHi);
                bufferSize = frameSize;
                frameLoBounded = false;
            }

            frameIncludesCurrentValue = rowsHi == 0;
            this.buffer = memory;
            try {
                initBuffer();
            } catch (Throwable t) {
                close();
                throw t;
            }
        }

        @Override
        public void close() {
            super.close();
            buffer.close();
        }

        @Override
        public void computeNext(Record record) {

            double price = arg1.getDouble(record);
            double volume = arg2.getDouble(record);

            //compute value using top frame element (that could be current or previous row)
            double hiValuePrice = price;
            double hiValueVolume = volume;

            if (frameLoBounded && !frameIncludesCurrentValue) {
                hiValuePrice = buffer.getDouble((long) ((loIdx + frameSize - 1) % bufferSize) * Double.BYTES);
                hiValueVolume = buffer.getDouble((long) ((loIdx + frameSize - 1) % bufferSize) * Double.BYTES);

            } else if (!frameLoBounded && !frameIncludesCurrentValue) {
                hiValuePrice = buffer.getDouble((long) (loIdx % bufferSize) * Double.BYTES);
                hiValueVolume = buffer.getDouble((long) (loIdx % bufferSize) * Double.BYTES);
            }

            if (Numbers.isFinite(hiValueVolume) && Numbers.isFinite(hiValuePrice) && hiValueVolume > 0.0d) {
                volumeSum += hiValueVolume;
                notional += hiValuePrice * hiValueVolume;
                count++;
            }

            if (count != 0) {
                vwma = notional / volumeSum;
                externalNotional = notional;
            } else {
                vwma = Double.NaN;
                externalNotional = Double.NaN;
            }

            if (frameLoBounded) {
                //remove the oldest element with newest
                double loValuePrice = buffer.getDouble((long) loIdx * Double.BYTES);
                double loValueVolume = buffer.getDouble((long) (loIdx + 1) * Double.BYTES);
                
                if (Numbers.isFinite(loValueVolume) && Numbers.isFinite(loValuePrice) && loValueVolume > 0) {
                    notional -= loValueVolume * loValuePrice;
                    volumeSum -= loValueVolume;

                    count--;
                                        
                }
            }

            buffer.putDouble((long) loIdx * Double.BYTES, price);
            buffer.putDouble((long) (loIdx + 1) * Double.BYTES, volume);
            loIdx = (loIdx + 1) % bufferSize;
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
            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), vwma);
        }

        @Override
        public void reopen() {
            vwma = 0;
            count = 0;
            loIdx = 0;
            notional = 0.0;
            initBuffer();
        }

        @Override
        public void reset() {
            super.reset();
            buffer.close();
            vwma = 0;
            count = 0;
            loIdx = 0;
            notional = 0.0;
            volumeSum = 0.0;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg1).val(')');
            sink.val('(').val(arg2).val(')');
            sink.val(" over (");
            sink.val(" rows between ");
            if (frameLoBounded) {
                sink.val(bufferSize);
            } else {
                sink.val("unbounded");
            }
            sink.val(" preceding and ");
            if (frameIncludesCurrentValue) {
                sink.val("current row");
            } else {
                sink.val(bufferSize - frameSize).val(" preceding");
            }
            sink.val(')');
        }

        @Override
        public void toTop() {
            super.toTop();
            vwma = 0;
            count = 0;
            loIdx = 0;
            notional = 0.0;
            volumeSum = 0.0;
            initBuffer();
        }

        private void initBuffer() {
            for (int i = 0; i < bufferSize; i++) {
                buffer.putDouble((long) i * Double.BYTES, Double.NaN);
            }
        }
    }
    
    static {
        VWMA_COLUMN_TYPES = new ArrayColumnTypes();
        VWMA_COLUMN_TYPES.add(ColumnType.DOUBLE);
        VWMA_COLUMN_TYPES.add(ColumnType.LONG);

        VWMA_OVER_PARTITION_ROWS_COLUMN_TYPES = new ArrayColumnTypes();
        VWMA_OVER_PARTITION_ROWS_COLUMN_TYPES.add(ColumnType.DOUBLE);// sum
        VWMA_OVER_PARTITION_ROWS_COLUMN_TYPES.add(ColumnType.LONG);// current frame size
        VWMA_OVER_PARTITION_ROWS_COLUMN_TYPES.add(ColumnType.LONG);// position of current oldest element
        VWMA_OVER_PARTITION_ROWS_COLUMN_TYPES.add(ColumnType.LONG);// start offset of native array
    }
}
