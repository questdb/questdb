package io.questdb.griffin.engine.functions.eq;

import io.questdb.cairo.sql.Function;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.NegatableBooleanFunction;
import io.questdb.std.str.CharSink;

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
    public void toSink(CharSink sink) {
        sink.put(left);
        if (negated) {
            sink.put('!');
        }
        sink.put('=').put(right);
    }
}
