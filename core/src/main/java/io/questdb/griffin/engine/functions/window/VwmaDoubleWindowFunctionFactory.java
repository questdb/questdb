package io.questdb.griffin.engine.functions.window;

import org.jetbrains.annotations.NotNull;

import io.questdb.cairo.*;
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

        int framingMode = windowContext.getFramingMode();
        if (framingMode == WindowColumn.FRAMING_GROUPS) {
            throw SqlException.$(position, "function not implemented for given window parameters");
        }

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
            initBuffer();
        }

        private void initBuffer() {
            for (int i = 0; i < bufferSize; i++) {
                buffer.putDouble((long) i * Double.BYTES, Double.NaN);
            }
        }
    }
}
