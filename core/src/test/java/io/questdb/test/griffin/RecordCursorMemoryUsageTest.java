/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package io.questdb.test.griffin;

import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.engine.groupby.*;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractGriffinTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class RecordCursorMemoryUsageTest extends AbstractGriffinTest {

    //HashJoinRecordCursorFactory

    @Test
    public void testAsOfJoinRecordCursorReleasesMemoryOnClose() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table tab as (select" +
                    " rnd_symbol(20,4,4,20000) sym1," +
                    " rnd_double(2) d," +
                    " timestamp_sequence(0, 1000000000) ts" +
                    " from long_sequence(1000)) timestamp(ts)");
            try {
                compiler.setFullFatJoins(true);
                try (RecordCursorFactory factory = compile("select * from tab t1 asof join tab t2 ")
                        .getRecordCursorFactory()) {

                    long freeDuring;
                    long memDuring;

                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        freeDuring = Unsafe.getFreeCount();
                        memDuring = getMemUsedByFactories();
                        TestUtils.drainCursor(cursor);
                    }

                    long memAfter = getMemUsedByFactories();
                    long freeAfter = Unsafe.getFreeCount();

                    Assert.assertTrue(memAfter < memDuring);
                    Assert.assertTrue(freeAfter > freeDuring);
                }
            } finally {
                compiler.setFullFatJoins(false);
            }
        });
    }

    @Test
    public void testSampleByFillNoneRecordCursorReleasesMemoryOnClose() throws Exception {
        testSampleByCursorReleasesMemoryOnClose("", SampleByFillNoneRecordCursorFactory.class);
    }

    @Test
    public void testSampleByFillNullRecordCursorReleasesMemoryOnClose() throws Exception { //prev / value
        testSampleByCursorReleasesMemoryOnClose("FILL(null)", SampleByFillNullRecordCursorFactory.class);
    }

    @Test
    public void testSampleByFillPrevRecordCursorReleasesMemoryOnClose() throws Exception {
        testSampleByCursorReleasesMemoryOnClose("FILL(prev)", SampleByFillPrevRecordCursorFactory.class);
    }

    @Test
    public void testSampleByFillValueRecordCursorReleasesMemoryOnClose() throws Exception { //prev / value
        testSampleByCursorReleasesMemoryOnClose("FILL(10)", SampleByFillValueRecordCursorFactory.class);
    }

    private void testSampleByCursorReleasesMemoryOnClose(String fill, Class<?> expectedFactoryClass) throws Exception {
        assertMemoryLeak(() -> {
            compile("create table tab as (select" +
                    " rnd_symbol(20,4,4,20000) sym1," +
                    " rnd_double(2) d," +
                    " timestamp_sequence(0, 1000000000) ts" +
                    " from long_sequence(10000)) timestamp(ts)");

            try (AbstractSampleByRecordCursorFactory factory = (AbstractSampleByRecordCursorFactory) compile("select sym1, sum(d) from tab SAMPLE BY 1d " + fill)
                    .getRecordCursorFactory()) {
                Assert.assertSame(factory.getClass(), expectedFactoryClass);

                long freeDuring;
                long memDuring;

                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    TestUtils.drainCursor(cursor);
                    freeDuring = Unsafe.getFreeCount();
                    memDuring = Unsafe.getMemUsed();
                }

                long memAfter = Unsafe.getMemUsed();
                long freeAfter = Unsafe.getFreeCount();

                Assert.assertTrue(memAfter < memDuring);
                Assert.assertTrue(freeAfter > freeDuring);
            }
        });
    }
}
