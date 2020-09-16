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
import io.questdb.std.Chars;
import io.questdb.std.Rnd;
import io.questdb.test.tools.TestUtils;
import org.junit.Before;
import org.junit.Test;

public class OutOfOrderTest extends AbstractGriffinTest {

    @Before
    public void setUp3() {
        SharedRandom.RANDOM.set(new Rnd());
    }

    @Test
    public void testPartitionedOOData() throws Exception {
        assertMemoryLeak(() -> {

                    // create table with roughly 2AM data
                    compiler.compile(
                            "create table x as (" +
                                    "select" +
                                    " cast(x as int) i," +
                                    " rnd_symbol('msft','ibm', 'googl') sym," +
                                    " round(rnd_double(0)*100, 3) amt," +
                                    " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp," +
                                    " rnd_boolean() b," +
                                    " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                                    " rnd_double(2) d," +
                                    " rnd_float(2) e," +
                                    " rnd_short(10,1024) f," +
                                    " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                                    " rnd_symbol(4,4,4,2) ik," +
                                    " rnd_long() j," +
                                    " timestamp_sequence(10000000000,1000000L) ts," +
                                    " rnd_byte(2,50) l," +
//                                    " rnd_bin(10, 20, 2) m," +
                                    " rnd_str(5,16,2) n," +
                                    " rnd_char() t" +
                                    " from long_sequence(500)" +
                                    ") timestamp (ts) partition by DAY",
                            sqlExecutionContext
                    );

                    // create table with 1AM data

                    compiler.compile(
                            "create table 1am as (" +
                                    "select" +
                                    " cast(x as int) i," +
                                    " rnd_symbol('msft','ibm', 'googl') sym," +
                                    " round(rnd_double(0)*100, 3) amt," +
                                    " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp," +
                                    " rnd_boolean() b," +
                                    " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                                    " rnd_double(2) d," +
                                    " rnd_float(2) e," +
                                    " rnd_short(10,1024) f," +
                                    " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                                    " rnd_symbol(4,4,4,2) ik," +
                                    " rnd_long() j," +
                                    " timestamp_sequence(0,1000000L) ts," +
                                    " rnd_byte(2,50) l," +
//                                    " rnd_bin(10, 20, 2) m," +
                                    " rnd_str(5,16,2) n," +
                                    " rnd_char() t" +
                                    " from long_sequence(500)" +
                                    ")",
                            sqlExecutionContext
                    );

                    // create third table, which will contain both X and 1AM
                    compiler.compile("create table y as (select * from x union all select * from 1am)", sqlExecutionContext);

                    // expected outcome
                    sink.clear();
                    try (RecordCursorFactory factory = compiler.compile("y order by ts", sqlExecutionContext).getRecordCursorFactory()) {
                        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            printer.print(cursor, factory.getMetadata(), true);
                        }
                    }

                    String expected = Chars.toString(sink);

                    // insert 1AM data into X
                    compiler.compile("insert into x select * from 1am", sqlExecutionContext);

                    sink.clear();
                    try (RecordCursorFactory factory = compiler.compile("x", sqlExecutionContext).getRecordCursorFactory()) {
                        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            printer.print(cursor, factory.getMetadata(), true);
                        }
                    }

                    TestUtils.assertEquals(expected, sink);
                }
        );
    }

    @Test
    public void testPartitionedOOMergeOO() throws Exception {
        assertMemoryLeak(() -> {
                    compiler.compile(
                            "create table x_1 as (" +
                                    "select" +
                                    " cast(x as int) i," +
                                    " rnd_symbol('msft','ibm', 'googl') sym," +
                                    " round(rnd_double(0)*100, 3) amt," +
                                    " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp," +
                                    " rnd_boolean() b," +
                                    " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                                    " rnd_double(2) d," +
                                    " rnd_float(2) e," +
                                    " rnd_short(10,1024) f," +
                                    " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                                    " rnd_symbol(4,4,4,2) ik," +
                                    " rnd_long() j," +
                                    " timestamp_shuffle(0,100000000000L) ts," +
                                    " rnd_byte(2,50) l," +
//                                    " rnd_bin(10, 20, 2) m," +
                                    " rnd_str(5,16,2) n," +
                                    " rnd_char() t" +
                                    " from long_sequence(1000)" +
                                    ")",
                            sqlExecutionContext
                    );

                    compiler.compile(
                            "create table x as (select * from x_1 order by ts) timestamp(ts) partition by DAY",
                            sqlExecutionContext
                    );

                    compiler.compile(
                            "create table y as (select * from x_1) timestamp(ts) partition by DAY",
                            sqlExecutionContext
                    );

                    final String sqlTemplate = "select i,sym,amt,timestamp,b,c,d,e,f,g,ik,ts,l,n,t from ";

                    sink.clear();
                    try (RecordCursorFactory factory = compiler.compile(sqlTemplate + "x", sqlExecutionContext).getRecordCursorFactory()) {
                        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            printer.print(cursor, factory.getMetadata(), true);
                        }
                    }

                    String expected = Chars.toString(sink);

                    sink.clear();
                    try (RecordCursorFactory factory = compiler.compile(sqlTemplate + "y", sqlExecutionContext).getRecordCursorFactory()) {
                        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            printer.print(cursor, factory.getMetadata(), true);
                        }
                    }
                    TestUtils.assertEquals(expected, sink);
                }
        );
    }
}
