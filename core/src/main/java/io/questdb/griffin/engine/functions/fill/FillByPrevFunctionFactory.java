// File: src/main/java/io/questdb/griffin/engine/functions/fill/FillByPrevFunctionFactory.java

package io.questdb.griffin.engine.functions.fill;

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.AbstractFunctionFactory;
import io.questdb.std.ObjList;

public class FillByPrevFunctionFactory extends AbstractFunctionFactory {
    @Override
    public String getSignature() {
        return "fill_prev(V)";
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, int argPos, SqlExecutionContext sqlExecutionContext) {
        return new FillByPrevFunction(args.getQuick(0));
    }

    private static class FillByPrevFunction extends AbstractFunction {
        private final Function value;

        public FillByPrevFunction(Function value) {
            this.value = value;
        }

        @Override
        public void compute(Record rec) {
            // Logic to fill the value by previous non-null value
            // Implement the logic to handle the FILL operation here
        }
    }
}
