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

package io.questdb.griffin;

import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.engine.functions.rnd.SharedRandom;
import io.questdb.std.Rnd;
import io.questdb.test.tools.TestUtils;
import org.junit.Before;
import org.junit.Test;

public class KeyedAggregationTest extends AbstractGriffinTest {
    @Before
    public void setUp3() {
        SharedRandom.RANDOM.set(new Rnd());
    }

    @Test
    public void testIntSymbolResolution() throws Exception {
        assertQuery(
                "s2\tsum\n" +
                        "\t104119.880948161\n" +
                        "a1\t103804.62242300605\n" +
                        "a2\t104433.68659571148\n" +
                        "a3\t104341.28852517322\n",
                "select s2, sum(val) from tab order by s2",
                "create table tab as (select rnd_symbol('s1','s2','s3', null) s1, rnd_symbol('a1','a2','a3', null) s2, rnd_double(2) val from long_sequence(1000000))",
                null, true
        );
    }

    @Test
    public void testIntSymbolAddKeyMidTable() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table tab as (select rnd_symbol('s1','s2','s3', null) s1, rnd_double(2) val from long_sequence(1000000))", sqlExecutionContext);
            compiler.compile("alter table tab add column s2 symbol cache", sqlExecutionContext);
            compiler.compile("insert into tab select rnd_symbol('s1','s2','s3', null), rnd_double(2), rnd_symbol('a1','a2','a3', null) s2 from long_sequence(1000000)", sqlExecutionContext);

            try (
                    RecordCursorFactory factory = compiler.compile("select s2, sum(val) from tab order by s2", sqlExecutionContext).getRecordCursorFactory();
                    RecordCursor cursor = factory.getCursor(sqlExecutionContext)
            ) {

                String expected = "s2\tsum\n" +
                        "\t520447.6629968713\n" +
                        "a1\t104308.65839619507\n" +
                        "a2\t104559.2867475151\n" +
                        "a3\t104044.11326997809\n";

                sink.clear();
                printer.print(cursor, factory.getMetadata(), true);
                TestUtils.assertEquals(expected, sink);
            }
        });
    }

    @Test
    public void testIntSymbolAddValueMidTable() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table tab as (select rnd_symbol('s1','s2','s3', null) s1 from long_sequence(1000000))", sqlExecutionContext);
            compiler.compile("alter table tab add column val double", sqlExecutionContext);
            compiler.compile("insert into tab select rnd_symbol('a1','a2','a3', null), rnd_double(2) from long_sequence(1000000)", sqlExecutionContext);

            try (
                    RecordCursorFactory factory = compiler.compile("select s1, sum(val) from tab order by s1", sqlExecutionContext).getRecordCursorFactory();
                    RecordCursor cursor = factory.getCursor(sqlExecutionContext)
            ) {

                String expected = "s1\tsum\n" +
                        "\t104083.77969067449\n" +
                        "a1\t103982.62399952614\n" +
                        "a2\t104702.89752880299\n" +
                        "a3\t104299.02298329721\n" +
                        "s1\tNaN\n" +
                        "s2\tNaN\n" +
                        "s3\tNaN\n";

                sink.clear();
                printer.print(cursor, factory.getMetadata(), true);
                TestUtils.assertEquals(expected, sink);
            }
        });
    }

    @Test
    public void testIntSymbolAddBothMidTable() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table tab as (select rnd_symbol('s1','s2','s3', null) s1 from long_sequence(1000000))", sqlExecutionContext);
            compiler.compile("alter table tab add column s2 symbol cache", sqlExecutionContext);
            compiler.compile("alter table tab add column val double", sqlExecutionContext);
            compiler.compile("insert into tab select null, rnd_symbol('a1','a2','a3', null), rnd_double(2) from long_sequence(1000000)", sqlExecutionContext);

            try (
                    RecordCursorFactory factory = compiler.compile("select s2, sum(val) from tab order by s2", sqlExecutionContext).getRecordCursorFactory();
                    RecordCursor cursor = factory.getCursor(sqlExecutionContext)
            ) {

                String expected = "s2\tsum\n" +
                        "\t104083.77969067449\n" +
                        "a1\t103982.62399952614\n" +
                        "a2\t104702.89752880299\n" +
                        "a3\t104299.02298329721\n";

                sink.clear();
                printer.print(cursor, factory.getMetadata(), true);
                TestUtils.assertEquals(expected, sink);
            }
        });
    }
}
