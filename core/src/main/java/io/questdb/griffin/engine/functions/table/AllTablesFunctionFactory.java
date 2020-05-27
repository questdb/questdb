package io.questdb.griffin.engine.functions.table;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.CursorFunction;
import io.questdb.griffin.engine.table.TableListRecordCursorFactory;
import io.questdb.std.ObjList;

public class AllTablesFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "all_tables()";
    }

    @Override
    public Function newInstance(ObjList<Function> args, int position, CairoConfiguration configuration) throws SqlException {
        return new CursorFunction(
                position,
                new TableListRecordCursorFactory(configuration.getFilesFacade(), configuration.getRoot()));
    }

}
