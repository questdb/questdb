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
import io.questdb.std.ObjList;

public class TestLatchedCounterFunctionFactory implements FunctionFactory {
	private static final AtomicInteger COUNTER = new AtomicInteger();
	private static volatile Callback CALLBACK;

	public static void reset(Callback callback) {
		CALLBACK = callback;
		COUNTER.set(0);
	}

	public static int getCount() {
		return COUNTER.get();
	}

	@Override
	public String getSignature() {
		return "test_latched_counter()";
	}

	@Override
	public Function newInstance(ObjList<Function> args, int position, CairoConfiguration configuration)
			throws SqlException {
		return new TestLatchFunction(position);
	}

	private static class TestLatchFunction extends BooleanFunction {
		private final Callback callback;

		public TestLatchFunction(int position) {
			super(position);
			callback = CALLBACK;
		}

		@Override
		public boolean getBool(Record rec) {
			int count = COUNTER.incrementAndGet();
			if (null == callback) {
				return true;
			}
			return callback.onGet(rec, count);
		}

		@Override
		public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) {
		}

		@Override
		public void close() {
			if (null != callback) {
				callback.onClose();
			}
		}

	}

	public interface Callback {
		default boolean onGet(Record rec, int count) {
			return true;
		}
		
		default void onClose() {
		}
	}
}
