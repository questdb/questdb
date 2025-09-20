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

package io.questdb.test.griffin.engine.groupby;

import io.questdb.PropertyKey;
import io.questdb.ServerMain;
import io.questdb.cairo.CairoEngine;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Chars;
import io.questdb.std.Misc;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;

public class SampleByNanoTimestampConfigTest extends AbstractBootstrapTest {

    final CharSequence ddl = "CREATE TABLE sensors (\n" +
            "ts TIMESTAMP_NS,\n" +
            "val INT\n" +
            ") TIMESTAMP(ts) PARTITION BY DAY WAL";
    final CharSequence dml = "INSERT INTO sensors (ts, val) VALUES \n" +
            "('2021-05-31T23:10:00.000000001Z', 10),\n" +
            "('2021-06-01T01:10:00.000000001Z', 80),\n" +
            "('2021-06-01T07:20:00.000000001Z', 15),\n" +
            "('2021-06-01T13:20:00.000000001Z', 10),\n" +
            "('2021-06-01T19:20:00.000000001Z', 40),\n" +
            "('2021-06-02T01:10:00.000000001Z', 90),\n" +
            "('2021-06-02T07:20:00.000000001Z', 30)";
    final CharSequence expectedCalendar = "ts\tcount\n" +
            "2021-05-31T00:00:00.000000000Z\t1\n" +
            "2021-06-01T00:00:00.000000000Z\t4\n" +
            "2021-06-02T00:00:00.000000000Z\t2\n";
    final CharSequence expectedFirstObservation = "ts\tcount\n" +
            "2021-05-31T23:10:00.000000001Z\t5\n" +
            "2021-06-01T23:10:00.000000001Z\t2\n";
    final CharSequence query = "SELECT ts, count from sensors\n" +
            "SAMPLE BY 1d";

    public SampleByNanoTimestampConfigTest() {

    }

    @Test
    public void testSampleByConfigDefaultAlignmentCalendar() throws Exception {
        testSampleByConfigDefaultAlignmentOption("calendar", true, expectedCalendar, query);
    }

    @Test
    public void testSampleByConfigDefaultAlignmentCommented() throws Exception {
        testSampleByConfigDefaultAlignmentOption("", true, expectedCalendar, query);
    }

    @Test
    public void testSampleByConfigDefaultAlignmentFirstObservation() throws Exception {
        testSampleByConfigDefaultAlignmentOption("first observation", false, expectedFirstObservation, query);
    }

    @Test
    public void testSampleByConfigDefaultAlignmentOverrideCalendar() throws Exception {
        testSampleByConfigDefaultAlignmentOption("calendar", true, expectedFirstObservation, query + " ALIGN TO FIRST OBSERVATION");
    }

    @Test
    public void testSampleByConfigDefaultAlignmentOverrideFirstObservation() throws Exception {
        testSampleByConfigDefaultAlignmentOption("first observation", false, expectedCalendar, query + " ALIGN TO CALENDAR");
    }

    private void testSampleByConfigDefaultAlignmentOption(CharSequence alignment, boolean expectedConfig, CharSequence expected, CharSequence query) throws Exception {
        TestUtils.unchecked(() -> {
            if (!Chars.empty(alignment)) {
                createDummyConfiguration(PropertyKey.CAIRO_SQL_SAMPLEBY_DEFAULT_ALIGNMENT_CALENDAR + "="
                        + Chars.equals(alignment, "calendar"));
            }
        });

        try (final ServerMain serverMain = ServerMain.createWithoutWalApplyJob(root, new HashMap<>())) {
            Assert.assertEquals(serverMain.getConfiguration().getCairoConfiguration().getSampleByDefaultAlignmentCalendar(), expectedConfig);
            serverMain.start();

            final CairoEngine engine = serverMain.getEngine();

            try (SqlExecutionContext sqlExecutionContext = TestUtils.createSqlExecutionCtx(engine)) {
                engine.execute(ddl, sqlExecutionContext);
                engine.execute(dml, sqlExecutionContext);
                drainWalQueue(engine);

                TestUtils.assertSql(engine, sqlExecutionContext, query, Misc.getThreadLocalSink(), expected);
            }
        }
    }
}
