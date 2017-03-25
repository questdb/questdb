package com.questdb.ql.ops.round;

import com.questdb.ex.NumericException;
import com.questdb.misc.Numbers;
import com.questdb.ql.Record;
import com.questdb.ql.ops.AbstractBinaryOperator;
import com.questdb.ql.ops.Function;
import com.questdb.ql.ops.VirtualColumnFactory;
import com.questdb.store.ColumnType;

public class RoundHalfUpFunction extends AbstractBinaryOperator {

    public final static VirtualColumnFactory<Function> FACTORY = (position, configuration) -> new RoundHalfUpFunction(position);

    private RoundHalfUpFunction(int position) {
        super(ColumnType.DOUBLE, position);
    }

    @Override
    public double getDouble(Record rec) {
        try {
            return Numbers.roundHalfUp(lhs.getDouble(rec), rhs.getInt(rec));
        } catch (NumericException e) {
            return Double.NaN;
        }
    }
}
