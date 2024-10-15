package io.questdb.griffin.engine.functions.groupby;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.DoubleFunction;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.std.Numbers;
import org.jetbrains.annotations.NotNull;

public class TwapDoubleGroupByFunction extends DoubleFunction implements GroupByFunction, BinaryFunction {
    private final Function priceFunction;
    private final Function timeFunction;
    private int valueIndex;

    public TwapDoubleGroupByFunction(@NotNull Function arg0, @NotNull Function arg1) {
        this.priceFunction = arg0;
        this.timeFunction = arg1;
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record, long rowId) {
        final double price = priceFunction.getDouble(record);
        final double time = timeFunction.getDouble(record);
        if (Numbers.isFinite(price) && Numbers.isFinite(time) && time > 0.0d) {
            final double notional = price * time;
            final double twap = notional / time;
            mapValue.putDouble(valueIndex, twap);
            mapValue.putDouble(valueIndex + 1, notional);
            mapValue.putDouble(valueIndex + 2, time);
        } else {
            setNull(mapValue);
        }
    }

    @Override
    public void computeNext(MapValue mapValue, Record record, long rowId) {
        final double price = priceFunction.getDouble(record);
        final double time = timeFunction.getDouble(record);
        if (Numbers.isFinite(price) && Numbers.isFinite(time) && time > 0.0d) {
            final double notional = price * time;
            mapValue.addDouble(valueIndex + 1, notional);
            mapValue.addDouble(valueIndex + 2, time);
            mapValue.putDouble(valueIndex, mapValue.getDouble(valueIndex + 1) / mapValue.getDouble(valueIndex + 2));
        }
    }

    @Override
    public double getDouble(Record rec) {
        return rec.getDouble(valueIndex);
    }

    @Override
    public Function getLeft() {
        return priceFunction;
    }

    @Override
    public Function getRight() {
        return timeFunction;
    }

    @Override
    public String getName() {
        return "twap";
    }

    @Override
    public int getValueIndex() {
        return valueIndex;
    }

    @Override
    public void initValueIndex(int valueIndex) {
        this.valueIndex = valueIndex;
    }

    @Override
    public void initValueTypes(ArrayColumnTypes columnTypes) {
        this.valueIndex = columnTypes.getColumnCount();
        columnTypes.add(ColumnType.DOUBLE);
        columnTypes.add(ColumnType.DOUBLE);
        columnTypes.add(ColumnType.DOUBLE);
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public boolean isReadThreadSafe() {
        return BinaryFunction.super.isReadThreadSafe();
    }

    @Override
    public void merge(MapValue destValue, MapValue srcValue) {
        double srcNotional = srcValue.getDouble(valueIndex + 1);
        double srcTime = srcValue.getDouble(valueIndex + 2);
        if (Numbers.isFinite(srcNotional) && Numbers.isFinite(srcTime) && srcTime > 0.0d) {
            destValue.addDouble(valueIndex + 1, srcNotional);
            destValue.addDouble(valueIndex + 2, srcTime);
            destValue.putDouble(valueIndex, destValue.getDouble(valueIndex + 1) / destValue.getDouble(valueIndex + 2));
        }
    }

    @Override
    public void setDouble(MapValue mapValue, double value) {
        mapValue.putDouble(valueIndex, value);
        mapValue.putDouble(valueIndex + 1, value);
        mapValue.putDouble(valueIndex + 2, 1);
    }

    @Override
    public void setNull(MapValue mapValue) {
        mapValue.putDouble(valueIndex, Double.NaN);
        mapValue.putDouble(valueIndex + 1, 0);
        mapValue.putDouble(valueIndex + 2, 0);
    }

    @Override
    public boolean supportsParallelism() {
        return BinaryFunction.super.supportsParallelism();
    }
}
