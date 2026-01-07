package io.questdb.test.griffin.fuzz;

import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.LimitRecordCursorFactory;
import io.questdb.griffin.engine.functions.constants.LongConstant;
import io.questdb.std.Numbers;
import io.questdb.std.Rnd;

public final class LimitOperatorFactory {
    /**
     * LIMIT can always be applied; ordering is handled by the generator.
     */
    public boolean supports(RecordCursorFactory input) {
        return true;
    }

    /**
     * Applies LIMIT with randomized bounds and mirrors it in the oracle.
     */
    public OperatorState apply(FuzzBuildContext context, OperatorState state, Rnd rnd) throws SqlException {
        LimitSpec spec = nextLimitSpec(rnd);
        state.underTest = new LimitRecordCursorFactory(
                state.underTest,
                LongConstant.newInstance(spec.lo),
                spec.hasHi ? LongConstant.newInstance(spec.hi) : null,
                0
        );
        state.oracle = new ReferenceLimitRecordCursorFactory(state.oracle, spec);
        return state;
    }

    private static LimitSpec nextLimitSpec(Rnd rnd) {
        int mode = rnd.nextInt(6);
        long lo;
        long hi;
        boolean hasHi;
        switch (mode) {
            case 0:
                hasHi = false;
                lo = rnd.nextInt(25);
                hi = Numbers.LONG_NULL;
                break;
            case 1:
                hasHi = false;
                lo = -1 - rnd.nextInt(25);
                hi = Numbers.LONG_NULL;
                break;
            case 2:
                hasHi = true;
                lo = rnd.nextInt(25);
                hi = rnd.nextInt(25);
                break;
            case 3:
                hasHi = true;
                lo = -1 - rnd.nextInt(25);
                hi = -1 - rnd.nextInt(25);
                break;
            case 4:
                hasHi = true;
                lo = rnd.nextInt(25);
                hi = -1 - rnd.nextInt(25);
                break;
            default:
                hasHi = true;
                lo = rnd.nextInt(25);
                hi = lo;
                break;
        }
        return new LimitSpec(lo, hi, hasHi);
    }
}
