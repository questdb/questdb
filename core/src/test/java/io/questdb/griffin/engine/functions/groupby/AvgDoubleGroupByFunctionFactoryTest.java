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

package io.questdb.griffin.engine.functions.groupby;

import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.AbstractGriffinTest;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.engine.functions.rnd.SharedRandom;
import io.questdb.std.Rnd;
import io.questdb.test.tools.TestUtils;
import org.junit.Before;
import org.junit.Test;

public class AvgDoubleGroupByFunctionFactoryTest extends AbstractGriffinTest {

    @Before
    public void setUp3() {
        SharedRandom.RANDOM.set(new Rnd());
    }

    @Test
    public void testAll() throws Exception {
        assertMemoryLeak(() -> {
            CompiledQuery cq = compiler.compile("select max(x), avg(x), sum(x) from long_sequence(10)", sqlExecutionContext);

            try (RecordCursorFactory factory = cq.getRecordCursorFactory()) {
                sink.clear();
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    printer.print(cursor, factory.getMetadata(), true);
                }
            }

            TestUtils.assertEquals("max\tavg\tsum\n" +
                            "10\t5.5\t55\n",
                    sink);
        });
    }

    @Test
    public void testAvgWithInfinity() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table test2 as(select case  when rnd_double() > 0.6 then 1.0   else 0.0  end val from long_sequence(100));", sqlExecutionContext);
            CompiledQuery cq = compiler.compile("select avg(1/val) from test2", sqlExecutionContext);

            try (RecordCursorFactory factory = cq.getRecordCursorFactory()) {
                sink.clear();
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    printer.print(cursor, factory.getMetadata(), true);
                }
            }
            TestUtils.assertEquals("avg\n1.0\n", sink);
        });
    }

    @Test
    public void testAllWithInfinity() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table test2 as(select case  when rnd_double() > 0.6 then 1.0   else 0.0  end val from long_sequence(100));", sqlExecutionContext);
            CompiledQuery cq = compiler.compile("select sum(1/val) , avg(1/val), max(1/val), min(1/val), ksum(1/val), nsum(1/val) from test2", sqlExecutionContext);

            try (RecordCursorFactory factory = cq.getRecordCursorFactory()) {
                sink.clear();
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    printer.print(cursor, factory.getMetadata(), true);
                }
            }
            TestUtils.assertEquals("sum\tavg\tmax\tmin\tksum\tnsum\n" +
                            "44.0\t1.0\tInfinity\t1.0\t44.0\t44.0\n",
                    sink);
        });
    }
}