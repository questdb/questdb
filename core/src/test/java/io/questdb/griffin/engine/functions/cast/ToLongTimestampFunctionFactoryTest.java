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

package io.questdb.griffin.engine.functions.cast;

import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.AbstractFunctionFactoryTest;
import io.questdb.std.Numbers;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

public class ToLongTimestampFunctionFactoryTest extends AbstractFunctionFactoryTest {
    static long testValue = 1262596503400000L;

    @Test
    public void testNan() throws SqlException {
        call(Numbers.LONG_NaN).andAssert(Numbers.LONG_NaN);
    }

    @Test
    public void testLong() throws SqlException {
        call(testValue).andAssert(testValue);
    }

    @Test
    public void testFullQuery() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            CompiledQuery res = compiler.compile("select to_long(to_timestamp(" + testValue  + "L)) from long_sequence(1)");
            RecordCursor cursor = res.getRecordCursorFactory().getCursor();
            Record record = cursor.getRecord();
            assert record.getLong(0) == testValue;
            engine.releaseAllWriters();
            engine.releaseAllReaders();
        });
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new ToLongTimestampFunctionFactory();
    }
}