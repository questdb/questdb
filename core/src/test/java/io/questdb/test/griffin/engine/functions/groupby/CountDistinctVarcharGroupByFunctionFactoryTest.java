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

package io.questdb.test.griffin.engine.functions.groupby;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class CountDistinctVarcharGroupByFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testNonKeyedHappy() throws Exception {
        String expected = """
                count_distinct
                3
                """;
        assertQuery(
                expected,
                "select count_distinct(a) from x",
                "create table x as (select * from (select rnd_varchar('a','b','c') a from long_sequence(20)))",
                null,
                false,
                true
        );
    }

    @Test
    public void testSampleBy() throws Exception {
        execute("create table x (ts timestamp, vch varchar) timestamp(ts);");
        execute("insert into x values ('2000-01-01', 'foo'), ('2000-01-01T04:30', 'bar'), ('2000-01-01T05:30', 'foobar'), ('2000-01-03', 'foo');");

        String expectedDefault = """
                ts\tcount_distinct
                2000-01-01T00:00:00.000000Z\t3
                2000-01-03T00:00:00.000000Z\t1
                """;
        assertQuery(
                expectedDefault,
                "select ts, count_distinct(vch) from x sample by 1d",
                "ts",
                true,
                true
        );

        String expectedInterpolated = """
                ts\tcount_distinct
                2000-01-01T00:00:00.000000Z\t3
                2000-01-02T00:00:00.000000Z\t2
                2000-01-03T00:00:00.000000Z\t1
                """;
        assertQuery(
                expectedInterpolated,
                "select ts, count_distinct(vch) from x sample by 1d fill(linear)",
                "ts",
                true,
                true
        );
    }

}
