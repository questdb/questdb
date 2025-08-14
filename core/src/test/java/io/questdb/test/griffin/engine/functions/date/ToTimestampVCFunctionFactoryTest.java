/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.test.griffin.engine.functions.date;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.columns.TimestampColumn;
import io.questdb.griffin.engine.functions.constants.StrConstant;
import io.questdb.griffin.engine.functions.constants.TimestampConstant;
import io.questdb.griffin.engine.functions.date.ToTimestampVCFunctionFactory;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.test.griffin.engine.AbstractFunctionFactoryTest;
import org.junit.Assert;
import org.junit.Test;

public class ToTimestampVCFunctionFactoryTest extends AbstractFunctionFactoryTest {
    @Test
    public void testNanosField() throws SqlException {
        // The "N+" specifier must accept nanoseconds, but the result type must still be `TIMESTAMP` (micros)
        call(".123456789", ".N+").andAssertTimestamp(123456);
    }

    @Test
    public void testNonCompliantDate() throws SqlException {
        call("2015 03/12 abc", "yyyy dd/MM").andAssertTimestamp(Numbers.LONG_NULL);
    }

    @Test
    public void testNullDate() throws SqlException {
        call(null, "yyyy dd/MM").andAssertTimestamp(Numbers.LONG_NULL);
    }

    @Test
    public void testNullPattern() {
        assertFailure(16, "pattern is required", "2015", null);
    }

    @Test
    public void testSimple() throws SqlException {
        call("2015 03/12 514", "yyyy dd/MM UUU").andAssertTimestamp(1449100800000514L);
    }

    @Test
    public void test_whenInputStringIsConstant_functionReturnsConstant() throws SqlException {
        ObjList<Function> funcs = new ObjList<>();
        funcs.add(new StrConstant("2021 01/04"));
        funcs.add(new StrConstant("yyyy dd/MM"));

        Function f = new ToTimestampVCFunctionFactory().newInstance(0, funcs, new IntList(), configuration, sqlExecutionContext);

        Assert.assertEquals(TimestampConstant.class, f.getClass());
    }

    @Test
    public void test_whenInputStringIsNotConstant_functionDoesntCacheValue() throws SqlException {
        ObjList<Function> funcs = new ObjList<>();
        funcs.add(TimestampColumn.newInstance(1, ColumnType.TIMESTAMP_MICRO));
        funcs.add(new StrConstant("yyyy dd/MM"));

        Function f = new ToTimestampVCFunctionFactory().newInstance(0, funcs, new IntList(), configuration, sqlExecutionContext);

        Assert.assertEquals("io.questdb.griffin.engine.functions.date.ToTimestampVCFunctionFactory$Func", f.getClass().getName());
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new ToTimestampVCFunctionFactory();
    }
}
