package io.questdb.griffin.engine.functions.window;

import org.jetbrains.annotations.NotNull;

import io.questdb.cairo.*;
import io.questdb.cairo.sql.*;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryARW;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.window.AvgDoubleWindowFunctionFactory.AvgOverCurrentRowFunction;
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
            return new AvgOverUnboundedRowsFrameFunction(args.get(0));
        } // between current row and current row
        else if (rowsLo == 0 && rowsLo == rowsHi) {
            return new AvgOverCurrentRowFunction(args.get(0));
        } // whole result set
        else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
            return new AvgOverWholeResultSetFunction(args.get(0));
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

    // Handles avg() over (rows between unbounded preceding and current row); there's no partition by.
    static class AvgOverUnboundedRowsFrameFunction extends BaseDoubleWindowFunction {

        private double avg;
        private long count = 0;
        private double sum = 0.0;

        public AvgOverUnboundedRowsFrameFunction(Function arg) {
            super(arg);
        }

        @Override
        public void computeNext(Record record) {
            double d = arg.getDouble(record);
            if (Numbers.isFinite(d)) {
                sum += d;
                count++;
            }

            avg = count != 0 ? sum / count : Double.NaN;
        }

        @Override
        public double getDouble(Record rec) {
            return avg;
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

            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), avg);
        }

        @Override
        public void reset() {
            super.reset();
            avg = Double.NaN;
            count = 0;
            sum = 0.0;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(NAME);
            sink.val('(').val(arg).val(')');
            sink.val(" over (rows between unbounded preceding and current row)");
        }

        @Override
        public void toTop() {
            super.toTop();

            avg = Double.NaN;
            count = 0;
            sum = 0.0;
        }
    }

    // avg() over () - empty clause, no partition by no order by, no frame == default frame
    static class AvgOverWholeResultSetFunction extends BaseDoubleWindowFunction {
        private double avg;
        private long count;
        private double sum;

        public AvgOverWholeResultSetFunction(Function arg) {
            super(arg);
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
            double d = arg.getDouble(record);
            if (Numbers.isFinite(d)) {
                sum += d;
                count++;
            }
        }

        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), avg);
        }

        @Override
        public void preparePass2() {
            avg = count > 0 ? sum / count : Double.NaN;
        }

        @Override
        public void reset() {
            super.reset();
            avg = Double.NaN;
            count = 0;
            sum = 0.0;
        }

        @Override
        public void toTop() {
            super.toTop();

            avg = Double.NaN;
            count = 0;
            sum = 0.0;
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
            
            assert rowsLo != Long.MIN_VALUE || rowsHi != 0; // use AvgOverUnboundedRowsFrameFunction in case of (Long.MIN_VALUE, 0) range
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

            System.out.println("PRICE "+ price);
            System.out.println("VOLUME "+ volume);

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

            System.out.println("HI VALUE PRICE "+ hiValuePrice);
            System.out.println("HI VALUE VOLUME "+ hiValueVolume);

            if (Numbers.isFinite(hiValueVolume) && Numbers.isFinite(hiValuePrice) && hiValueVolume > 0.0d) {
                volumeSum += hiValueVolume;
                notional += hiValuePrice * hiValueVolume;
                count++;
            }

            System.out.println("VOLUME SUM "+ volumeSum);
            System.out.println("NOTIONAL "+ notional);

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

                    System.out.println("NOTIONAL POST REMOVE "+ notional);
                    System.out.println("VOLUMESUM POST REMOVE "+ volumeSum);

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
