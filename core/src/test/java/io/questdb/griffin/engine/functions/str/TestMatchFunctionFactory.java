/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package io.questdb.griffin.engine.functions.str;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.std.ObjList;

import java.util.concurrent.atomic.AtomicInteger;

public class TestMatchFunctionFactory implements FunctionFactory {

    private static final AtomicInteger openCounter = new AtomicInteger();
    private static final AtomicInteger topCounter = new AtomicInteger();
    private static final AtomicInteger closeCount = new AtomicInteger();

    public static void clear() {
        openCounter.set(0);
        topCounter.set(0);
        closeCount.set(0);
    }

    public static int getCloseCount() {
        return closeCount.get();
    }

    public static int getOpenCount() {
        return openCounter.get();
    }

    public static int getTopCount() {
        return topCounter.get();
    }

    @Override
    public String getSignature() {
        return "test_match()";
    }

    @Override
    public Function newInstance(ObjList<Function> args, int position, CairoConfiguration configuration) {
        return new TestMatchFunction(position);
    }

    private static class TestMatchFunction extends BooleanFunction {

        public TestMatchFunction(int position) {
            super(position);
        }

        @Override
        public void close() {
            closeCount.incrementAndGet();
        }

        @Override
        public boolean getBool(Record rec) {
            return true;
        }

        @Override
        public void init(RecordCursor recordCursor, SqlExecutionContext sqlExecutionContext) {
            openCounter.incrementAndGet();
        }

        @Override
        public boolean isConstant() {
            return false;
        }

        @Override
        public void toTop() {
            assert openCounter.get() > 0;
            topCounter.incrementAndGet();
        }
    }
}
