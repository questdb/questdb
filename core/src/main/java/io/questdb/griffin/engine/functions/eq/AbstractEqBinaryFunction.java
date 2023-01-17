package io.questdb.griffin.engine.functions.eq;

import io.questdb.cairo.sql.Function;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.NegatableBooleanFunction;

public abstract class AbstractEqBinaryFunction extends NegatableBooleanFunction implements BinaryFunction {
    protected final Function left;
    protected final Function right;

    public AbstractEqBinaryFunction(Function left, Function right) {
        this.left = left;
        this.right = right;
    }

    @Override
    public Function getLeft() {
        return left;
    }

    @Override
    public Function getRight() {
        return right;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.val(left);
        if (negated) {
            sink.val('!');
        }
        sink.val('=').val(right);
    }
}
