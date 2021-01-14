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

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.engine.functions.rnd.SharedRandom;
import io.questdb.std.Chars;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;

public class OutOfOrderTest extends AbstractGriffinTest {

    @Before
    public void setUp3() {
        configuration = new DefaultCairoConfiguration(root) {
            @Override
            public boolean isOutOfOrderEnabled() {
                return true;
            }
        };

        engine = new CairoEngine(configuration);
        compiler = new SqlCompiler(engine);
        sqlExecutionContext = new SqlExecutionContextImpl(
                engine, 1)
                .with(
                        AllowAllCairoSecurityContext.INSTANCE,
                        bindVariableService,
                        null,
                        -1,
                        null);
        bindVariableService.clear();

        SharedRandom.RANDOM.set(new Rnd());
    }

    @Test
    public void testPartitionedDataAppendOOData() throws Exception {
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
                                    " timestamp_sequence(500000000000L,100000000L) ts," +
                                    " rnd_byte(2,50) l," +
                                    " rnd_bin(10, 20, 2) m," +
                                    " rnd_str(5,16,2) n," +
                                    " rnd_char() t" +
                                    " from long_sequence(500)" +
                                    "), index(sym) timestamp (ts) partition by DAY",
                            sqlExecutionContext
                    );

                    compiler.compile(
                            "create table append as (" +
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
                                    " timestamp_sequence(518300000010L,100000L) ts," +
                                    " rnd_byte(2,50) l," +
                                    " rnd_bin(10, 20, 2) m," +
                                    " rnd_str(5,16,2) n," +
                                    " rnd_char() t" +
                                    " from long_sequence(100)" +
                                    ") timestamp (ts) partition by DAY",
                            sqlExecutionContext
                    );

                    // create third table, which will contain both X and 1AM
                    assertOutOfOrderDataConsistency(
                            "create table y as (x union all append)",
                            "y order by ts",
                            "insert into x select * from append",
                            "x"
                    );

                    assertIndexConsistency();
                }
        );
    }

    @Test
    public void testPartitionedDataAppendOODataIndexed() throws Exception {
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
                                    " timestamp_sequence(500000000000L,100000000L) ts," +
                                    " rnd_byte(2,50) l," +
                                    " rnd_bin(10, 20, 2) m," +
                                    " rnd_str(5,16,2) n," +
                                    " rnd_char() t" +
                                    " from long_sequence(500)" +
                                    "), index(sym) timestamp (ts) partition by DAY",
                            sqlExecutionContext
                    );

                    compiler.compile(
                            "create table append as (" +
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
                                    " timestamp_sequence(518300000010L,100000L) ts," +
                                    " rnd_byte(2,50) l," +
                                    " rnd_bin(10, 20, 2) m," +
                                    " rnd_str(5,16,2) n," +
                                    " rnd_char() t" +
                                    " from long_sequence(100)" +
                                    ") timestamp (ts) partition by DAY",
                            sqlExecutionContext
                    );

                    // create third table, which will contain both X and 1AM
                    assertOutOfOrderDataConsistency(
                            "create table y as (x union all append)",
                            "y where sym = 'googl' order by ts",
                            "insert into x select * from append",
                            "x where sym = 'googl'"
                    );
                }
        );
    }

    @Test
    public void testPartitionedDataAppendOODataNotNullStrTail() throws Exception {
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
                                    " timestamp_sequence(500000000000L,100000000L) ts," +
                                    " rnd_byte(2,50) l," +
                                    " cast(null as binary) m," +
                                    " rnd_str(5,16,2) n," +
                                    " rnd_char() t" +
                                    " from long_sequence(510)" +
                                    "), index(sym) timestamp (ts) partition by DAY",
                            sqlExecutionContext
                    );

                    compiler.compile(
                            "create table append as (" +
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
                                    " timestamp_sequence(518300000010L,100000L) ts," +
                                    " rnd_byte(2,50) l," +
                                    " rnd_bin(10, 20, 2) m," +
                                    " rnd_str(5,16,2) n," +
                                    " rnd_char() t" +
                                    " from long_sequence(100)" +
                                    ") timestamp (ts) partition by DAY",
                            sqlExecutionContext
                    );

                    assertOutOfOrderDataConsistency(
                            "create table y as (x union all append)",
                            "y order by ts",
                            "insert into x select * from append",
                            "x"
                    );

                    assertIndexConsistency();
                }
        );
    }

    @Test
    public void testPartitionedDataAppendOOPrependOOData() throws Exception {
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
                                    " timestamp_sequence(500000000000L,100000000L) ts," +
                                    " rnd_byte(2,50) l," +
                                    " cast(null as binary) m," +
                                    " rnd_str(5,16,2) n," +
                                    " rnd_char() t" +
                                    " from long_sequence(510)" +
                                    "), index(sym) timestamp (ts) partition by DAY",
                            sqlExecutionContext
                    );

                    // all records but one is appended to middle partition
                    // last record is prepended to the last partition
                    compiler.compile(
                            "create table append as (" +
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
                                    " timestamp_sequence(518390000000L,100000L) ts," +
                                    " rnd_byte(2,50) l," +
                                    " rnd_bin(10, 20, 2) m," +
                                    " rnd_str(5,16,2) n," +
                                    " rnd_char() t" +
                                    " from long_sequence(101)" +
                                    ") timestamp (ts) partition by DAY",
                            sqlExecutionContext
                    );

                    // create third table, which will contain both X and 1AM
                    assertOutOfOrderDataConsistency(
                            "create table y as (x union all append)",
                            "y order by ts",
                            "insert into x select * from append",
                            "x"
                    );

                    assertIndexConsistency();
                }
        );
    }

    @Test
    public void testPartitionedDataMergeData() throws Exception {
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
                                    " timestamp_sequence(500000000000L,1000000L) ts," +
                                    " rnd_byte(2,50) l," +
                                    " rnd_bin(10, 20, 2) m," +
                                    " rnd_str(5,16,2) n," +
                                    " rnd_char() t" +
                                    " from long_sequence(500)" +
                                    "), index(sym) timestamp (ts) partition by DAY",
                            sqlExecutionContext
                    );

                    compiler.compile(
                            "create table middle as (" +
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
                                    " timestamp_sequence(500288000000L,100000L) ts," +
                                    " rnd_byte(2,50) l," +
                                    " rnd_bin(10, 20, 2) m," +
                                    " rnd_str(5,16,2) n," +
                                    " rnd_char() t" +
                                    " from long_sequence(100)" +
                                    ") timestamp (ts) partition by DAY",
                            sqlExecutionContext
                    );

                    // create third table, which will contain both X and 1AM
                    assertOutOfOrderDataConsistency(
                            "create table y as (x union all middle)",
                            "insert into x select * from middle",
                            "/oo/testPartitionedDataMergeData.txt"
                    );
            assertIndexResultAgainstFile("/oo/testPartitionedDataMergeData_Index.txt");
                }
        );
    }

    @Test
    public void testPartitionedDataMergeEnd() throws Exception {
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
                                    " timestamp_sequence(500000000000L,1000000L) ts," +
                                    " rnd_byte(2,50) l," +
                                    " rnd_bin(10, 20, 2) m," +
                                    " rnd_str(5,16,2) n," +
                                    " rnd_char() t" +
                                    " from long_sequence(295)" +
                                    "), index(sym) timestamp (ts) partition by DAY",
                            sqlExecutionContext
                    );

                    compiler.compile(
                            "create table middle as (" +
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
                                    " timestamp_sequence(500288000000L,100000L) ts," +
                                    " rnd_byte(2,50) l," +
                                    " rnd_bin(10, 20, 2) m," +
                                    " rnd_str(5,16,2) n," +
                                    " rnd_char() t" +
                                    " from long_sequence(61)" + // <--- these counts are important to align the data
                                    ") timestamp (ts) partition by DAY",
                            sqlExecutionContext
                    );

                    assertOutOfOrderDataConsistency(
                            "create table y as (x union all middle)",
                            "insert into x select * from middle",
                            "/oo/testPartitionedDataMergeEnd.txt"
                    );

                    assertIndexResultAgainstFile("/oo/testPartitionedDataMergeEnd_Index.txt");
                }
        );
    }

    @Test
    public void testPartitionedDataOOData() throws Exception {
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
                                    " timestamp_sequence(500000000000L,1000000L) ts," +
                                    " rnd_byte(2,50) l," +
                                    " rnd_bin(10, 20, 2) m," +
                                    " rnd_str(5,16,2) n," +
                                    " rnd_char() t" +
                                    " from long_sequence(500)" +
                                    "), index(sym) timestamp (ts) partition by DAY",
                            sqlExecutionContext
                    );

                    // create table with 1AM data

                    compiler.compile(
                            "create table middle as (" +
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
                                    " timestamp_sequence(500288000000L,10L) ts," +
                                    " rnd_byte(2,50) l," +
                                    " rnd_bin(10, 20, 2) m," +
                                    " rnd_str(5,16,2) n," +
                                    " rnd_char() t" +
                                    " from long_sequence(500)" +
                                    ") timestamp (ts) partition by DAY",
                            sqlExecutionContext
                    );

                    // create third table, which will contain both X and 1AM
                    assertOutOfOrderDataConsistency(
                            "create table y as (x union all middle)",
                            "insert into x select * from middle",
                            "/oo/testPartitionedDataOOData.txt"
                    );

                    assertIndexConsistency();
                }
        );
    }

    @Test
    public void testPartitionedDataOODataPbOOData() throws Exception {
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
                                    " timestamp_sequence(10000000000,100000000L) ts," +
                                    " rnd_byte(2,50) l," +
                                    " rnd_bin(10, 20, 2) m," +
                                    " rnd_str(5,16,2) n," +
                                    " rnd_char() t" +
                                    " from long_sequence(1000)" +
                                    "), index(sym) timestamp (ts) partition by DAY",
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
                                    " timestamp_sequence(72700000000L,1000000L) ts," + // mid partition for "x"
                                    " rnd_byte(2,50) l," +
                                    " rnd_bin(10, 20, 2) m," +
                                    " rnd_str(5,16,2) n," +
                                    " rnd_char() t" +
                                    " from long_sequence(100)" +
                                    ")",
                            sqlExecutionContext
                    );

                    compiler.compile(
                            "create table top2 as (" +
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
                                    " cast(86400000000L as timestamp) ts," + // these row should go on top of second partition
                                    " rnd_byte(2,50) l," +
                                    " rnd_bin(10, 20, 2) m," +
                                    " rnd_str(5,16,2) n," +
                                    " rnd_char() t" +
                                    " from long_sequence(100)" +
                                    ")",
                            sqlExecutionContext
                    );

                    // create third table, which will contain both X and 1AM
                    assertOutOfOrderDataConsistency(
                            "create table y as (select * from x union all select * from 1am union all select * from top2)",
                            "insert into x select * from (1am union all top2)",
                            "/oo/testPartitionedDataOODataPbOOData.txt"
                    );

                    assertIndexResultAgainstFile("/oo/testPartitionedDataOODataPbOOData_Index.txt");
                }
        );
    }

    @Test
    public void testPartitionedDataOOIntoLastIndexSearchBug() throws Exception {
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
                                    " rnd_bin(10, 20, 2) m," +
                                    " rnd_str(5,16,2) n," +
                                    " rnd_char() t" +
                                    " from long_sequence(1000)" +
                                    "), index(sym) timestamp (ts) partition by DAY",
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
                                    " timestamp_sequence(3000000000l,10000000L) ts," + // mid partition for "x"
                                    " rnd_byte(2,50) l," +
                                    " rnd_bin(10, 20, 2) m," +
                                    " rnd_str(5,16,2) n," +
                                    " rnd_char() t" +
                                    " from long_sequence(1000)" +
                                    ")",
                            sqlExecutionContext
                    );

                    compiler.compile(
                            "create table tail as (" +
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
                                    " timestamp_sequence(20000000000,1000000L) ts," +
                                    " rnd_byte(2,50) l," +
                                    " rnd_bin(10, 20, 2) m," +
                                    " rnd_str(5,16,2) n," +
                                    " rnd_char() t" +
                                    " from long_sequence(100)" +
                                    ") timestamp (ts) partition by DAY",
                            sqlExecutionContext
                    );

                    // create third table, which will contain both X and 1AM
                    compiler.compile("create table y as (x union all 1am union all tail)", sqlExecutionContext);

                    // expected outcome
                    sink.clear();
                    try (RecordCursorFactory factory = compiler.compile("y order by ts", sqlExecutionContext).getRecordCursorFactory()) {
                        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            printer.print(cursor, factory.getMetadata(), true);
                        }
                    }

                    final String expected = Chars.toString(sink);

                    // close readers to allow "rename" to succeed on Windows
                    engine.releaseAllReaders();

                    // The query above generates expected result, but there is a problem using it
                    // This test produces duplicate timestamps. Those are being sorted in different order by OOO implementation
                    // and the reference query. So they cannot be directly compared. The parts with duplicate timestamps will
                    // look different. If this test ever breaks, uncomment the reference query and compare results visually.

                    // insert 1AM data into X
                    compiler.compile("insert into x select * from 1am", sqlExecutionContext);
                    compiler.compile("insert into x select * from tail", sqlExecutionContext);

                    // It is necessary to release cached "x" reader because as of yet
                    // reader cannot reload any partition other than "current".
                    // Cached reader will assume smaller partition size for "1970-01-01", which out-of-order insert updated
                    engine.releaseAllReaders();

                    sink.clear();
                    try (RecordCursorFactory factory = compiler.compile("x", sqlExecutionContext).getRecordCursorFactory()) {
                        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            printer.print(cursor, factory.getMetadata(), true);
                        }
                    }

                    TestUtils.assertEquals(expected, sink);

                    assertIndexConsistency();
                }
        );
    }

    @Test
    public void testPartitionedDataOOIntoLastOverflowIntoNewPartition() throws Exception {
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
                                    " rnd_bin(10, 20, 2) m," +
                                    " rnd_str(5,16,2) n," +
                                    " rnd_char() t" +
                                    " from long_sequence(1000)" +
                                    "), index(sym) timestamp (ts) partition by DAY",
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
                                    " timestamp_sequence(3000000000l,100000000L) ts," + // mid partition for "x"
                                    " rnd_byte(2,50) l," +
                                    " rnd_bin(10, 20, 2) m," +
                                    " rnd_str(5,16,2) n," +
                                    " rnd_char() t" +
                                    " from long_sequence(1000)" +
                                    ")",
                            sqlExecutionContext
                    );

                    compiler.compile(
                            "create table tail as (" +
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
                                    " timestamp_sequence(40000000000,1000000L) ts," +
                                    " rnd_byte(2,50) l," +
                                    " rnd_bin(10, 20, 2) m," +
                                    " rnd_str(5,16,2) n," +
                                    " rnd_char() t" +
                                    " from long_sequence(100)" +
                                    ") timestamp (ts) partition by DAY",
                            sqlExecutionContext
                    );

                    // create third table, which will contain both X and 1AM
                    compiler.compile("create table y as (x union all 1am union all tail)", sqlExecutionContext);

                    // expected outcome
                    sink.clear();
                    try (RecordCursorFactory factory = compiler.compile("y order by ts", sqlExecutionContext).getRecordCursorFactory()) {
                        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            printer.print(cursor, factory.getMetadata(), true);
                        }
                    }

//                    String expected = Chars.toString(sink);
                    engine.releaseAllReaders();

                    compiler.compile("insert into x select * from 1am", sqlExecutionContext);
                    compiler.compile("insert into x select * from tail", sqlExecutionContext);

                    // It is necessary to release cached "x" reader because as of yet
                    // reader cannot reload any partition other than "current".
                    // Cached reader will assume smaller partition size for "1970-01-01", which out-of-order insert updated
                    engine.releaseAllReaders();

                    assertSqlResultAgainstFile("x", "/oo/testPartitionedDataOOIntoLastOverflowIntoNewPartition.txt");

                    assertIndexConsistency();
                }
        );
    }

    @Test
    public void testBench() throws Exception {
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
                                    " rnd_bin(10, 20, 2) m," +
                                    " rnd_str(5,16,2) n," +
                                    " rnd_char() t" +
                                    " from long_sequence(1000000)" +
                                    "), index(sym) timestamp (ts) partition by DAY",
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
                                    " timestamp_sequence(3000000000l,100000000L) ts," + // mid partition for "x"
                                    " rnd_byte(2,50) l," +
                                    " rnd_bin(10, 20, 2) m," +
                                    " rnd_str(5,16,2) n," +
                                    " rnd_char() t" +
                                    " from long_sequence(1000000)" +
                                    ")",
                            sqlExecutionContext
                    );

                    engine.releaseAllReaders();

                    compiler.compile("insert into x select * from 1am", sqlExecutionContext);
                }
        );
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
                                    " rnd_bin(10, 20, 2) m," +
                                    " rnd_str(5,16,2) n," +
                                    " rnd_char() t" +
                                    " from long_sequence(500)" +
                                    "), index(sym) timestamp (ts) partition by DAY",
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
                                    " rnd_bin(10, 20, 2) m," +
                                    " rnd_str(5,16,2) n," +
                                    " rnd_char() t" +
                                    " from long_sequence(500)" +
                                    ")",
                            sqlExecutionContext
                    );

                    compiler.compile(
                            "create table tail as (" +
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
                                    " timestamp_sequence(40000000000,1000000L) ts," +
                                    " rnd_byte(2,50) l," +
                                    " rnd_bin(10, 20, 2) m," +
                                    " rnd_str(5,16,2) n," +
                                    " rnd_char() t" +
                                    " from long_sequence(100)" +
                                    ") timestamp (ts) partition by DAY",
                            sqlExecutionContext
                    );

                    // create third table, which will contain both X and 1AM
                    compiler.compile("create table y as (select * from x union all 1am union all tail)", sqlExecutionContext);

                    // expected outcome
                    sink.clear();
                    try (RecordCursorFactory factory = compiler.compile("y order by ts", sqlExecutionContext).getRecordCursorFactory()) {
                        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            printer.print(cursor, factory.getMetadata(), true);
                        }
                    }

                    String expected = Chars.toString(sink);

                    engine.releaseAllReaders();

                    // insert 1AM data into X
                    compiler.compile("insert into x select * from 1am", sqlExecutionContext);
                    compiler.compile("insert into x select * from tail", sqlExecutionContext);

                    sink.clear();
                    try (RecordCursorFactory factory = compiler.compile("x", sqlExecutionContext).getRecordCursorFactory()) {
                        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            printer.print(cursor, factory.getMetadata(), true);
                        }
                    }

                    TestUtils.assertEquals(expected, sink);

                    engine.releaseAllReaders();

                    sink.clear();
                    try (RecordCursorFactory factory = compiler.compile("x", sqlExecutionContext).getRecordCursorFactory()) {
                        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            printer.print(cursor, factory.getMetadata(), true);
                        }
                    }

                    TestUtils.assertEquals(expected, sink);

                    assertIndexConsistency();
                }
        );
    }

    @Test
    public void testPartitionedOODataOOCollapsed() throws Exception {
        assertMemoryLeak(() -> {
                    // top edge of data timestamp equals of of
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
                                    " timestamp_sequence(500288000000L,10L) ts," +
                                    " rnd_byte(2,50) l," +
                                    " rnd_bin(10, 20, 2) m," +
                                    " rnd_str(5,16,2) n," +
                                    " rnd_char() t" +
                                    " from long_sequence(500)" +
                                    "), index(sym) timestamp (ts) partition by DAY",
                            sqlExecutionContext
                    );

                    // create table with 1AM data

                    compiler.compile(
                            "create table middle as (" +
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
                                    " timestamp_sequence(500000000000L,1000000L) ts," +
                                    " rnd_byte(2,50) l," +
                                    " rnd_bin(10, 20, 2) m," +
                                    " rnd_str(5,16,2) n," +
                                    " rnd_char() t" +
                                    " from long_sequence(500)" +
                                    ") timestamp (ts) partition by DAY",
                            sqlExecutionContext
                    );

                    // create third table, which will contain both X and 1AM
                    assertOutOfOrderDataConsistency(
                            "create table y as (x union all middle)",
                            "y order by ts",
                            "insert into x select * from middle",
                            "x"
                    );

                    assertIndexConsistency();
                }
        );
    }

    @Test
    public void testPartitionedOODataUpdateMinTimestamp() throws Exception {
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
                                    " timestamp_sequence(500000000000L,1000000L) ts," +
                                    " rnd_byte(2,50) l," +
                                    " rnd_bin(10, 20, 2) m," +
                                    " rnd_str(5,16,2) n," +
                                    " rnd_char() t" +
                                    " from long_sequence(500)" +
                                    "), index(sym) timestamp (ts) partition by DAY",
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
                                    " timestamp_sequence(450000000000L,1000000L) ts," +
                                    " rnd_byte(2,50) l," +
                                    " rnd_bin(10, 20, 2) m," +
                                    " rnd_str(5,16,2) n," +
                                    " rnd_char() t" +
                                    " from long_sequence(500)" +
                                    ") timestamp (ts) partition by DAY",
                            sqlExecutionContext
                    );

                    compiler.compile(
                            "create table prev as (" +
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
                                    " timestamp_sequence(0L,100000000L) ts," +
                                    " rnd_byte(2,50) l," +
                                    " rnd_bin(10, 20, 2) m," +
                                    " rnd_str(5,16,2) n," +
                                    " rnd_char() t" +
                                    " from long_sequence(3000)" +
                                    ") timestamp (ts) partition by DAY",
                            sqlExecutionContext
                    );

                    // create third table, which will contain both X and 1AM
                    assertOutOfOrderDataConsistency(
                            "create table y as (select * from x union all select * from 1am union all select * from prev)",
                            "y order by ts",
                            "insert into x select * from (1am union all prev)",
                            "x"
                    );

                    assertIndexConsistency();
                }
        );
    }

    @Test
    public void testPartitionedOOMerge() throws Exception {
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
                                    " rnd_bin(10, 20, 2) m," +
                                    " rnd_str(5,16,2) n," +
                                    " rnd_char() t" +
                                    " from long_sequence(500)" +
                                    "), index(sym) timestamp (ts) partition by DAY",
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
                                    " timestamp_sequence(9993000000,1000000L) ts," +
                                    " rnd_byte(2,50) l," +
                                    " rnd_bin(10, 20, 2) m," +
                                    " rnd_str(5,16,2) n," +
                                    " rnd_char() t" +
                                    " from long_sequence(507)" +
                                    ")",
                            sqlExecutionContext
                    );

                    compiler.compile(
                            "create table tail as (" +
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
                                    " timestamp_sequence(20000000000,1000000L) ts," +
                                    " rnd_byte(2,50) l," +
                                    " rnd_bin(10, 20, 2) m," +
                                    " rnd_str(5,16,2) n," +
                                    " rnd_char() t" +
                                    " from long_sequence(100)" +
                                    ") timestamp (ts) partition by DAY",
                            sqlExecutionContext
                    );

                    // create third table, which will contain both X and 1AM
                    compiler.compile("create table y as (x union all 1am union all tail)", sqlExecutionContext);

                    // expected outcome
                    sink.clear();
                    try (RecordCursorFactory factory = compiler.compile("y order by ts", sqlExecutionContext).getRecordCursorFactory()) {
                        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            printer.print(cursor, factory.getMetadata(), true);
                        }
                    }

                    String expected = Chars.toString(sink);

                    // close reader to enable "rename" to succeed
                    engine.releaseAllReaders();

                    // insert 1AM data into X
                    compiler.compile("insert into x select * from 1am", sqlExecutionContext);
                    compiler.compile("insert into x select * from tail", sqlExecutionContext);

                    sink.clear();
                    try (RecordCursorFactory factory = compiler.compile("x", sqlExecutionContext).getRecordCursorFactory()) {
                        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            printer.print(cursor, factory.getMetadata(), true);
                        }
                    }

                    TestUtils.assertEquals(expected, sink);

                    //
                    engine.releaseAllReaders();

                    sink.clear();
                    try (RecordCursorFactory factory = compiler.compile("x", sqlExecutionContext).getRecordCursorFactory()) {
                        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            printer.print(cursor, factory.getMetadata(), true);
                        }
                    }

                    TestUtils.assertEquals(expected, sink);

                    assertIndexConsistency();
                }
        );
    }

    @Test
    public void testPartitionedOOMergeData() throws Exception {
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
                                    " rnd_bin(10, 20, 2) m," +
                                    " rnd_str(5,16,2) n," +
                                    " rnd_char() t" +
                                    " from long_sequence(1500)" +
                                    "), index(sym) timestamp (ts) partition by DAY",
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
                                    " timestamp_sequence(1000000000L,100000000L) ts," +
                                    // 1000000000L
                                    // 2200712364240
                                    " rnd_byte(2,50) l," +
                                    " rnd_bin(10, 20, 2) m," +
                                    " rnd_str(5,16,2) n," +
                                    " rnd_char() t" +
                                    " from long_sequence(100)" +
                                    ")",
                            sqlExecutionContext
                    );

                    compiler.compile(
                            "create table tail as (" +
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
                                    " timestamp_sequence(11500000000L,1000000L) ts," +
                                    " rnd_byte(2,50) l," +
                                    " rnd_bin(10, 20, 2) m," +
                                    " rnd_str(5,16,2) n," +
                                    " rnd_char() t" +
                                    " from long_sequence(100)" +
                                    ") timestamp (ts) partition by DAY",
                            sqlExecutionContext
                    );

                    // create third table, which will contain both X and 1AM
                    compiler.compile("create table y as (select * from x union all 1am union all tail)", sqlExecutionContext);

                    // expected outcome
                    sink.clear();
                    try (RecordCursorFactory factory = compiler.compile("y order by ts", sqlExecutionContext).getRecordCursorFactory()) {
                        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            printer.print(cursor, factory.getMetadata(), true);
                        }
                    }

                    String expected = Chars.toString(sink);

                    engine.releaseAllReaders();

                    // insert 1AM data into X
                    compiler.compile("insert into x select * from 1am", sqlExecutionContext);
                    compiler.compile("insert into x select * from tail", sqlExecutionContext);

                    sink.clear();
                    try (RecordCursorFactory factory = compiler.compile("x", sqlExecutionContext).getRecordCursorFactory()) {
                        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            printer.print(cursor, factory.getMetadata(), true);
                        }
                    }

                    TestUtils.assertEquals(expected, sink);

                    assertIndexConsistency();
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
                                    " rnd_bin(10, 20, 2) m," +
                                    " rnd_str(5,16,2) n," +
                                    " rnd_char() t" +
                                    " from long_sequence(1000)" +
                                    ")",
                            sqlExecutionContext
                    );

                    compiler.compile(
                            "create table y as (select * from x_1 order by ts) timestamp(ts) partition by DAY",
                            sqlExecutionContext
                    );

                    compiler.compile(
                            "create table x as (select * from x_1), index(sym) timestamp(ts) partition by DAY",
                            sqlExecutionContext
                    );

                    final String sqlTemplate = "select i,sym,amt,timestamp,b,c,d,e,f,g,ik,ts,l,n,t,m from ";

                    sink.clear();
                    try (RecordCursorFactory factory = compiler.compile(sqlTemplate + "y", sqlExecutionContext).getRecordCursorFactory()) {
                        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            printer.print(cursor, factory.getMetadata(), true);
                        }
                    }

                    String expected = Chars.toString(sink);

                    sink.clear();
                    try (RecordCursorFactory factory = compiler.compile(sqlTemplate + "x", sqlExecutionContext).getRecordCursorFactory()) {
                        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            printer.print(cursor, factory.getMetadata(), true);
                        }
                    }
                    TestUtils.assertEquals(expected, sink);

                    assertIndexConsistency();
                }
        );
    }

    @Test
    public void testPartitionedOOONullSetters() throws Exception {
        assertMemoryLeak(() -> {

            compiler.compile("create table x (a int, b int, c int, ts timestamp) timestamp(ts) partition by DAY", sqlExecutionContext);
            try (TableWriter w = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "x")) {
                TableWriter.Row r;

                r = w.newRow(TimestampFormatUtils.parseUTCTimestamp("2013-02-10T00:10:00.000000Z"));
                r.putInt(2, 30);
                r.append();

                r = w.newRow(TimestampFormatUtils.parseUTCTimestamp("2013-02-10T00:05:00.000000Z"));
                r.putInt(2, 10);
                r.append();

                r = w.newRow(TimestampFormatUtils.parseUTCTimestamp("2013-02-10T00:06:00.000000Z"));
                r.putInt(2, 20);
                r.append();

                w.commit();

                r = w.newRow(TimestampFormatUtils.parseUTCTimestamp("2013-02-10T00:11:00.000000Z"));
                r.putInt(2, 40);
                r.append();

                w.commit();
            }

            sink.clear();
            try (RecordCursorFactory factory = compiler.compile("x", sqlExecutionContext).getRecordCursorFactory()) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    printer.print(cursor, factory.getMetadata(), true);
                }
            }

            final String expected = "a\tb\tc\tts\n" +
                    "NaN\tNaN\t10\t2013-02-10T00:05:00.000000Z\n" +
                    "NaN\tNaN\t20\t2013-02-10T00:06:00.000000Z\n" +
                    "NaN\tNaN\t30\t2013-02-10T00:10:00.000000Z\n" +
                    "NaN\tNaN\t40\t2013-02-10T00:11:00.000000Z\n";

            TestUtils.assertEquals(expected, sink);
        });
    }

    @Test
    public void testPartitionedOOPrefixesExistingPartitions() throws Exception {
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
                                    " timestamp_sequence(500000000000L,1000000L) ts," +
                                    " rnd_byte(2,50) l," +
                                    " rnd_bin(10, 20, 2) m," +
                                    " rnd_str(5,16,2) n," +
                                    " rnd_char() t" +
                                    " from long_sequence(500)" +
                                    "), index(sym) timestamp (ts) partition by DAY",
                            sqlExecutionContext
                    );

                    // create table with 1AM data

                    compiler.compile(
                            "create table top as (" +
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
                                    " timestamp_sequence(15000000000L,100000000L) ts," +
                                    " rnd_byte(2,50) l," +
                                    " rnd_bin(10, 20, 2) m," +
                                    " rnd_str(5,16,2) n," +
                                    " rnd_char() t" +
                                    " from long_sequence(1000)" +
                                    ") timestamp (ts) partition by DAY",
                            sqlExecutionContext
                    );

                    // create third table, which will contain both X and 1AM
                    assertOutOfOrderDataConsistency(
                            "create table y as (select * from x union all select * from top)",
                            "y order by ts",
                            "insert into x select * from top",
                            "x"
                    );

                    assertIndexConsistency();
                }
        );
    }

    @Test
    public void testPartitionedOOTopAndBottom() throws Exception {
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
                                    " timestamp_sequence(500000000000L,1000000L) ts," +
                                    " rnd_byte(2,50) l," +
                                    " rnd_bin(10, 20, 2) m," +
                                    " rnd_str(5,16,2) n," +
                                    " rnd_char() t" +
                                    " from long_sequence(500)" +
                                    "), index(sym) timestamp (ts) partition by DAY",
                            sqlExecutionContext
                    );

                    // create table with 1AM data

                    compiler.compile(
                            "create table top as (" +
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
                                    " timestamp_sequence(150000000000L,1000000L) ts," +
                                    " rnd_byte(2,50) l," +
                                    " rnd_bin(10, 20, 2) m," +
                                    " rnd_str(5,16,2) n," +
                                    " rnd_char() t" +
                                    " from long_sequence(500)" +
                                    ") timestamp (ts) partition by DAY",
                            sqlExecutionContext
                    );

                    compiler.compile(
                            "create table bottom as (" +
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
                                    " timestamp_sequence(500500000000L,100000000L) ts," +
                                    " rnd_byte(2,50) l," +
                                    " rnd_bin(10, 20, 2) m," +
                                    " rnd_str(5,16,2) n," +
                                    " rnd_char() t" +
                                    " from long_sequence(500)" +
                                    ") timestamp (ts) partition by DAY",
                            sqlExecutionContext
                    );

                    // create third table, which will contain both X and 1AM
                    assertOutOfOrderDataConsistency(
                            "create table y as (select * from x union all select * from top union all select * from bottom)",
                            "y order by ts",
                            "insert into x select * from (top union all bottom)",
                            "x"
                    );

                    assertIndexConsistency();
                }
        );
    }

    private void assertIndexConsistency() throws SqlException {
        // index test
        // expected outcome
        sink.clear();
        try (RecordCursorFactory factory = compiler.compile("y where sym = 'googl' order by ts", sqlExecutionContext).getRecordCursorFactory()) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                printer.print(cursor, factory.getMetadata(), true);
            }
        }

        String expected = Chars.toString(sink);

        sink.clear();
        try (RecordCursorFactory factory = compiler.compile("x where sym = 'googl'", sqlExecutionContext).getRecordCursorFactory()) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                printer.print(cursor, factory.getMetadata(), true);
            }
        }

        TestUtils.assertEquals(expected, sink);
    }

    private void assertIndexResultAgainstFile(String resourceName) throws SqlException, URISyntaxException {
        // file contains output of this SQL
        // we use file comparison because result set containing duplicate timestamps
        // order of records with duplicate timestamps is non-deterministic
/*
                    sink.clear();
                    try (RecordCursorFactory factory = compiler.compile("y where sym = 'googl' order by ts", sqlExecutionContext).getRecordCursorFactory()) {
                        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            printer.print(cursor, factory.getMetadata(), true);
                        }
                    }

                    String expected = Chars.toString(sink);
*/
        assertSqlResultAgainstFile("x where sym = 'googl'", resourceName);
    }

    private void assertOutOfOrderDataConsistency(
            final String referenceTableDDL,
            final String outOfOrderSQL,
            final String resourceName
    ) throws SqlException, URISyntaxException {
        // create third table, which will contain both X and 1AM
        compiler.compile(referenceTableDDL, sqlExecutionContext);

        // expected outcome - output ignored, but useful for debug
        sink.clear();
        try (RecordCursorFactory factory = compiler.compile("y order by ts", sqlExecutionContext).getRecordCursorFactory()) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                printer.print(cursor, factory.getMetadata(), true);
            }
        }

        engine.releaseAllReaders();

        compiler.compile(outOfOrderSQL, sqlExecutionContext);

        // release reader
        engine.releaseAllReaders();

        assertSqlResultAgainstFile("x", resourceName);
    }

    private void assertOutOfOrderDataConsistency(
            final String referenceTableDDL,
            final String referenceSQL,
            final String outOfOrderInsertSQL,
            final String assertSQL
    ) throws SqlException {
        // create third table, which will contain both X and 1AM
        compiler.compile(referenceTableDDL, sqlExecutionContext);

        // expected outcome
        sink.clear();
        try (RecordCursorFactory factory = compiler.compile(referenceSQL, sqlExecutionContext).getRecordCursorFactory()) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                printer.print(cursor, factory.getMetadata(), true);
            }
        }

        // uncomment these to look at result comparison from two queries
        // we are using file comparison here because of ordering issue on the identical timestamps
        String expected = Chars.toString(sink);

        // release reader "before" out-of-order is handled
        // we aim directory rename operations to succeed on Windows
        // Linux should be fine without closing readers
        engine.releaseAllReaders();

        compiler.compile(outOfOrderInsertSQL, sqlExecutionContext);

        // todo: ensure reader can pick up out of order stuff
        // release reader for now because it is unable to reload out-of-order results
        engine.releaseAllReaders();

        sink.clear();
        try (RecordCursorFactory factory = compiler.compile(assertSQL, sqlExecutionContext).getRecordCursorFactory()) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                printer.print(cursor, factory.getMetadata(), true);
            }
        }

        TestUtils.assertEquals(expected, sink);
    }

    private void assertSqlResultAgainstFile(String sql, String resourceName) throws URISyntaxException, SqlException {
        sink.clear();
        try (RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                printer.print(cursor, factory.getMetadata(), true);
            }
        }

        URL url = OutOfOrderTest.class.getResource(resourceName);
        Assert.assertNotNull(url);
        TestUtils.assertEquals(new File(url.toURI()), sink);
    }
}
