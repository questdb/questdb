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
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

public class AvgDoubleGroupByFunctionFactoryTest extends AbstractGriffinTest {

    @Test
    public void testAll() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            CompiledQuery cq = compiler.compile("select max(x), avg(x), sum(x) from long_sequence(10)");

            try (RecordCursorFactory factory = cq.getRecordCursorFactory()) {
                sink.clear();
                try (RecordCursor cursor = factory.getCursor()) {
                    printer.print(cursor, factory.getMetadata(), true);
                }
            }

            TestUtils.assertEquals("max\tavg\tsum\n" +
                            "10.0\t5.5\t55\n",
                    sink);

            engine.releaseAllWriters();
            engine.releaseAllReaders();
        });
    }

}