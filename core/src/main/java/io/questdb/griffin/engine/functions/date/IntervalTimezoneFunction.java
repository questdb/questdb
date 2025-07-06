package io.questdb.griffin.engine.functions.datetime;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.ObjList;
import io.questdb.std.time.TimeZoneRules;

public class IntervalTimezoneFunction extends UnaryFunction {
    private final Function intervalArg;
    private final CharSequence timezone;
    private final TimeZoneRules tzRules;

    public static class Factory implements FunctionFactory {
        @Override
        public String getSignature() {
            return "to_timezone(Is)";
        }

        @Override
        public Function newInstance(ObjList<Function> args, int position, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
            if (args.size() != 2) {
                throw new IllegalArgumentException("to_timezone expects 2 arguments: interval and timezone");
            }
            return new IntervalTimezoneFunction(args.getQuick(0), args.getQuick(1).getStr(null), configuration);
        }
    }

    public IntervalTimezoneFunction(Function intervalArg, CharSequence timezone, CairoConfiguration configuration) {
        super(intervalArg);
        this.intervalArg = intervalArg;
        this.timezone = timezone;
        this.tzRules = configuration.getTimeZoneRulesFactory().getRules(timezone);
        if (tzRules == null) {
            throw new IllegalArgumentException("Invalid timezone: " + timezone);
        }
    }

    @Override
    public long getIntervalStart(Record rec) {
        long start = intervalArg.getIntervalStart(rec);
        return tzRules.convertToLocal(start);
    }

    @Override
    public long getIntervalEnd(Record rec) {
        long end = intervalArg.getIntervalEnd(rec);
        return tzRules.convertToLocal(end);
    }

    @Override
    public boolean isRuntimeConstant() {
        return intervalArg.isRuntimeConstant() && timezone != null;
    }
}