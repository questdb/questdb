package io.questdb.test.griffin.fuzz;

import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.std.ObjList;

public final class WindowSpec {
    public final long lo;
    public final long hi;
    public final boolean includePrevailing;
    public final ObjList<GroupByFunction> groupByFunctions;

    public WindowSpec(long lo, long hi, boolean includePrevailing, ObjList<GroupByFunction> groupByFunctions) {
        this.lo = lo;
        this.hi = hi;
        this.includePrevailing = includePrevailing;
        this.groupByFunctions = groupByFunctions;
    }
}
