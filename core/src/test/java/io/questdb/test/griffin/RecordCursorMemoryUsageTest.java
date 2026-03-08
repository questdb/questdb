/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.engine.groupby.SampleByFillNoneRecordCursorFactory;
import io.questdb.griffin.engine.groupby.SampleByFillNullRecordCursorFactory;
import io.questdb.griffin.engine.groupby.SampleByFillPrevRecordCursorFactory;
import io.questdb.griffin.engine.groupby.SampleByFillValueRecordCursorFactory;
import io.questdb.griffin.engine.table.SelectedRecordCursorFactory;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class RecordCursorMemoryUsageTest extends AbstractCairoTest {

    //HashJoinRecordCursorFactory

    @Test
    public void testAsOfJoinRecordCursorReleasesMemoryOnClose() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab as (select" +
                    " rnd_symbol(20,4,4,20000) sym1," +
                    " rnd_double(2) d," +
                    " timestamp_sequence(0, 1000000000) ts" +
                    " from long_sequence(1000)) timestamp(ts)");
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                compiler.setFullFatJoins(true);
                try (RecordCursorFactory factory = compiler.compile("select * from tab t1 asof join tab t2;", sqlExecutionContext).getRecordCursorFactory()) {
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
            }
        });
    }

    @Test
    public void testSampleByFillNoneRecordCursorReleasesMemoryOnCloseCalendar() throws Exception {
        testSampleByCursorReleasesMemoryOnClose("", SelectedRecordCursorFactory.class, "CALENDAR");
    }

    @Test
    public void testSampleByFillNoneRecordCursorReleasesMemoryOnCloseFirstObservation() throws Exception {
        testSampleByCursorReleasesMemoryOnClose("", SampleByFillNoneRecordCursorFactory.class, "FIRST OBSERVATION");

    }

    @Test
    public void testSampleByFillNullRecordCursorReleasesMemoryOnCloseCalendar() throws Exception { //prev / value
        testSampleByCursorReleasesMemoryOnClose("FILL(null)", SampleByFillNullRecordCursorFactory.class, "CALENDAR");
    }

    @Test
    public void testSampleByFillNullRecordCursorReleasesMemoryOnCloseFirstObservation() throws Exception { //prev / value
        testSampleByCursorReleasesMemoryOnClose("FILL(null)", SampleByFillNullRecordCursorFactory.class, "FIRST OBSERVATION");
    }

    @Test
    public void testSampleByFillPrevRecordCursorReleasesMemoryOnCloseCalendar() throws Exception {
        testSampleByCursorReleasesMemoryOnClose("FILL(prev)", SampleByFillPrevRecordCursorFactory.class, "CALENDAR");
    }

    @Test
    public void testSampleByFillPrevRecordCursorReleasesMemoryOnFirstObservation() throws Exception {
        testSampleByCursorReleasesMemoryOnClose("FILL(prev)", SampleByFillPrevRecordCursorFactory.class, "FIRST OBSERVATION");
    }

    @Test
    public void testSampleByFillValueRecordCursorReleasesMemoryOnCloseCalendar() throws Exception { //prev / value
        testSampleByCursorReleasesMemoryOnClose("FILL(10)", SampleByFillValueRecordCursorFactory.class, "CALENDAR");
    }

    @Test
    public void testSampleByFillValueRecordCursorReleasesMemoryOnCloseFirstObservtion() throws Exception { //prev / value
        testSampleByCursorReleasesMemoryOnClose("FILL(10)", SampleByFillValueRecordCursorFactory.class, "FIRST OBSERVATION");
    }

    private void testSampleByCursorReleasesMemoryOnClose(String fill, Class<?> expectedFactoryClass, String alignment) throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab as (select" +
                    " rnd_symbol(20,4,4,20000) sym1," +
                    " rnd_double(2) d," +
                    " timestamp_sequence(0, 1000000000) ts" +
                    " from long_sequence(10000)) timestamp(ts)");

            try (RecordCursorFactory factory = select("select sym1, sum(d) from tab SAMPLE BY 1d " + fill + " ALIGN TO " + alignment)) {
                Assert.assertSame(expectedFactoryClass, factory.getBaseFactory().getClass());

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
