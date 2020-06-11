package io.questdb.griffin.engine.functions.test;

import java.util.concurrent.atomic.AtomicInteger;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.mp.SOUnboundedCountDownLatch;
import io.questdb.std.ObjList;

public class TestLatchedCounterFunctionFactory implements FunctionFactory {
    private static final SOUnboundedCountDownLatch START_LATCH = new SOUnboundedCountDownLatch();
    private static final SOUnboundedCountDownLatch CLOSED_LATCH = new SOUnboundedCountDownLatch();
    private static final AtomicInteger COUNTER = new AtomicInteger();

    public static void reset() {
        START_LATCH.reset();
        CLOSED_LATCH.reset();
        COUNTER.set(0);
    }

    public static void start() {
        START_LATCH.countDown();
    }

    public static void awaitClosed() {
        CLOSED_LATCH.await(1);
    }

    public static int getCount() {
        return COUNTER.get();
    }

    @Override
    public String getSignature() {
        return "test_latched_counter()";
    }

    @Override
    public Function newInstance(ObjList<Function> args, int position, CairoConfiguration configuration) throws SqlException {
        return new TestLatchFunction(position);
    }

    private static class TestLatchFunction extends BooleanFunction {

        public TestLatchFunction(int position) {
            super(position);
        }

        @Override
        public boolean getBool(Record rec) {
            START_LATCH.await(1);
            COUNTER.incrementAndGet();
            return true;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) {
        }

        @Override
        public void close() {
            CLOSED_LATCH.countDown();
        }

    }
}
