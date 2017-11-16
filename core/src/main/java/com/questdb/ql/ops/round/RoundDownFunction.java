package com.questdb.ql.ops.round;

import com.questdb.common.ColumnType;
import com.questdb.common.NumericException;
import com.questdb.common.Record;
import com.questdb.ql.ops.AbstractBinaryOperator;
import com.questdb.ql.ops.Function;
import com.questdb.ql.ops.VirtualColumnFactory;
import com.questdb.std.Numbers;

public class RoundDownFunction extends AbstractBinaryOperator {

    public final static VirtualColumnFactory<Function> FACTORY = (position, configuration) -> new RoundDownFunction(position);

    private RoundDownFunction(int position) {
        super(ColumnType.DOUBLE, position);
    }

    @Override
    public double getDouble(Record rec) {
        try {
            return Numbers.roundDown(lhs.getDouble(rec), rhs.getInt(rec));
        } catch (NumericException e) {
            return Double.NaN;
        }
    }
}
