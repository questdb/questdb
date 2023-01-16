package io.questdb.griffin.engine.groupby.vect;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.functions.LongFunction;
import io.questdb.std.Rosti;
import io.questdb.std.Unsafe;

import java.util.concurrent.atomic.LongAdder;


public abstract class AbstractCountVectorAggregateFunction extends LongFunction implements VectorAggregateFunction {
    protected final LongAdder aggCount = new LongAdder();//counts number of null key aggregations 
    protected final LongAdder count = new LongAdder();
    private final int columnIndex;
    protected DistinctFunc distinctFunc;
    protected KeyValueFunc keyValueFunc;
    protected int valueOffset;

    public AbstractCountVectorAggregateFunction(int columnIndex) {
        this.columnIndex = columnIndex;
    }

    @Override
    public void clear() {
        count.reset();
        aggCount.reset();
    }

    @Override
    public int getColumnIndex() {
        return columnIndex;
    }

    @Override
    public long getLong(Record rec) {
        return count.sum();
    }

    @Override
    public int getValueOffset() {
        return valueOffset;
    }

    @Override
    public void initRosti(long pRosti) {
        Unsafe.getUnsafe().putLong(Rosti.getInitialValueSlot(pRosti, valueOffset), 0);
    }

    @Override
    public boolean isReadThreadSafe() {
        return false;
    }

    @Override
    public boolean merge(long pRostiA, long pRostiB) {
        return Rosti.keyedIntCountMerge(pRostiA, pRostiB, valueOffset);
    }

    @Override
    public void pushValueTypes(ArrayColumnTypes types) {
        this.valueOffset = types.getColumnCount();
        types.add(ColumnType.LONG);
    }

    @Override
    public boolean wrapUp(long pRosti) {
        return Rosti.keyedIntCountWrapUp(pRosti, valueOffset, aggCount.sum() > 0 ? count.sum() : Long.MIN_VALUE);
    }
}
