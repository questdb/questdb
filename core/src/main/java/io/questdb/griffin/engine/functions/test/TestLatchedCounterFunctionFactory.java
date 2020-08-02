/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.griffin.engine.functions.test;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.std.ObjList;

import java.util.concurrent.atomic.AtomicInteger;

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
	public Function newInstance(ObjList<Function> args, int position, CairoConfiguration configuration) {
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
