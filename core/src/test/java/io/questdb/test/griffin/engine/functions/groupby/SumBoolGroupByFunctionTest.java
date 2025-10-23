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

package io.questdb.test.griffin.engine.functions.groupby;


import io.questdb.test.griffin.TimestampQueryTest;
import org.junit.Test;

public class SumBoolGroupByFunctionTest extends TimestampQueryTest {

    @Test
    public void testGreaterThan() throws Exception {
        execute("create table tt (i int)");
        execute("insert into tt values(2)");
        execute("insert into tt values(2)");
        execute("insert into tt values(0)");
        String query = "select sum(i>1) from tt";
        String expected = "sum\n" +
                "2\n";
        assertSql(expected, query);
    }

    @Test
    public void testEqualsTo() throws Exception {
        execute("create table tt (i int)");
        execute("insert into tt values(2)");
        execute("insert into tt values(2)");
        execute("insert into tt values(0)");
        String query = "select sum(i=0) from tt";
        String expected = "sum\n" +
                "1\n";
        assertSql(expected, query);
    }

    @Test
    public void testLessThan() throws Exception {
        execute("create table tt (i int)");
        execute("insert into tt values(2)");
        execute("insert into tt values(2)");
        execute("insert into tt values(3)");
        String expected = "sum\n" +
                "2\n";
        assertSql(expected, "select sum(i<3) from tt");
    }

    @Test
    public void testNotEqualsTo() throws Exception {
        execute("create table tt (i double)");
        execute("insert into tt values(2.6)");
        execute("insert into tt values(22.2)");
        execute("insert into tt values(3)");
        String expected = "sum\n" +
                "2\n";
        assertSql(expected, "select sum(i!=3) from tt");
    }


    @Test
    public void testTimestampInBetween() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tt (dts timestamp, nts timestamp) timestamp(dts)");
            // insert same values to dts (designated) as nts (non-designated) timestamp
            execute("insert into tt " +
                    "select timestamp_sequence(1577836800000000L, 60*60*1000000L), timestamp_sequence(1577836800000000L, 60*60*1000000L) " +
                    "from long_sequence(48L)");

            String expected = "sum\n" +
                    "48\n";
            assertSql(expected, "select sum(nts BETWEEN '2020-01-01' AND NOW()) from tt");
        });
    }

    @Test
    public void testTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tt (dts timestamp, nts timestamp) timestamp(dts)");
            // insert same values to dts (designated) as nts (non-designated) timestamp
            execute("insert into tt " +
                    "select timestamp_sequence(1577836800000000L, 60*60*1000000L), timestamp_sequence(1577836800000000L, 60*60*1000000L) " +
                    "from long_sequence(48L)");

            String expected = "sum\n" +
                    "0\n";
            assertSql(expected, "select sum(nts NOT BETWEEN '2020-01-01' AND NOW()) from tt");
        });
    }
}