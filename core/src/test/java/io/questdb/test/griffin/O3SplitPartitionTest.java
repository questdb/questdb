/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

import io.questdb.cairo.CairoEngine;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Os;
import io.questdb.std.Vect;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class O3SplitPartitionTest extends AbstractO3Test {
    protected static final Log LOG = LogFactory.getLog(O3SplitPartitionTest.class);
    private final StringBuilder tstData = new StringBuilder();
    private final int workerCount;
    @Rule
    public TestName name = new TestName();

    public O3SplitPartitionTest(ParallelMode mode) {
        this.workerCount = mode == ParallelMode.Contended ? 0 : 2;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {ParallelMode.Parallel},
                {ParallelMode.Contended}
        });
    }

    @Before
    public void setUp4() {
        partitionO3SplitThreshold = 1000;
        Vect.resetPerformanceCounters();
    }

    @After
    public void tearDown4() {
        int count = Vect.getPerformanceCountersCount();
        if (count > 0) {
            tstData.setLength(0);
            tstData.append(name.getMethodName()).append(",");
            long total = 0;
            for (int i = 0; i < count; i++) {
                long val = Vect.getPerformanceCounter(i);
                tstData.append(val).append("\t");
                total += val;
            }
            tstData.append(total);

            Os.sleep(10);
            System.err.flush();
            System.err.println(tstData);
            System.err.flush();
        }
    }

    @Test
    public void testDoubleSplitSamePartitionAtSameTransaction() throws Exception {
        executeWithPool(workerCount,
                (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext executionContext) -> {
                    compiler.compile(
                            "create table x as (" +
                                    "select" +
                                    " cast(x as int) i," +
                                    " -x j," +
                                    " rnd_str(5,16,2) as str," +
                                    " timestamp_sequence('2020-02-03T13', 60*1000000L) ts" +
                                    " from long_sequence(60*24*2+300)" +
                                    ") timestamp (ts) partition by DAY",
                            executionContext
                    );
                    compiler.compile("alter table x add column k int", executionContext).execute(null).await();
                    compiler.compile("alter table x add column sym symbol index ", executionContext).execute(null).await();
                    compiler.compile("alter table x add column ks string", executionContext).execute(null).await();

                    compiler.compile(
                            "create table y as (" +
                                    "select" +
                                    " cast(x as int) * 1000000 i," +
                                    " -x - 1000000L as j," +
                                    " rnd_str(5,16,2) as str," +
                                    " timestamp_sequence('2020-02-05T17:01:05', 60*1000000L) ts," +
                                    " 1 as k," +
                                    " rnd_symbol(null,'5','16','2') as sym," +
                                    " rnd_str(5,16,2) as ks" +
                                    " from long_sequence(1000))",
                            executionContext
                    );

                    compiler.compile(
                            "create table z as (" +
                                    "select" +
                                    " cast(x as int) * 1000000 i," +
                                    " -x - 1000000L as j," +
                                    " rnd_str(5,16,2) as str," +
                                    " timestamp_sequence('2020-02-05T14:01:07', 60*1000000L) ts," +
                                    " 1 as k," +
                                    " rnd_symbol(null,'5','16','2') as sym," +
                                    " rnd_str(5,16,2) as ks" +
                                    " from long_sequence(1000))",
                            executionContext
                    );

                    compiler.compile(
                            "create table zz (" +
                                    "i int," +
                                    "j long," +
                                    "str string," +
                                    "ts timestamp, " +
                                    "k int," +
                                    "sym symbol," +
                                    "ks string)",
                            executionContext
                    );

                    compiler.compile(
                            "insert into zz select * from x union all select * from y union all select * from z",
                            executionContext
                    );

                    compiler.compile("insert into x select * from y", executionContext);
                    compiler.compile("insert into x select * from z", executionContext);

                    assertX(compiler, executionContext, "zz");
                    assertIndex(compiler, executionContext, "zz", "x", "sym = '5'");
                    assertIndex(compiler, executionContext, "zz", "x", "sym is null");
                });
    }

    @Test
    public void testRebuildIndexLastPartitionWithColumnTop() throws Exception {
        executeWithPool(workerCount, (engine, compiler, sqlExecutionContext) -> {
            partitionO3SplitThreshold = 5;

            compiler.compile(
                    "CREATE TABLE monthly_col_top(" +
                            "ts timestamp, metric SYMBOL, diagnostic SYMBOL, sensorChannel SYMBOL" +
                            ") timestamp(ts) partition by MONTH",
                    sqlExecutionContext);

            compiler.compile(
                    "INSERT INTO monthly_col_top (ts, metric, diagnostic, sensorChannel) VALUES" +
                            "('2022-06-08T01:40:00.000000Z', '1', 'true', '2')," +
                            "('2022-06-08T02:41:00.000000Z', '2', 'true', '2')," +
                            "('2022-06-08T02:42:00.000000Z', '3', 'true', '1')," +
                            "('2022-06-08T02:43:00.000000Z', '4', 'true', '1')",
                    sqlExecutionContext).execute(null).await();

            compiler.compile("ALTER TABLE monthly_col_top ADD COLUMN loggerChannel SYMBOL INDEX", sqlExecutionContext)
                    .execute(null).await();

            compiler.compile("INSERT INTO monthly_col_top (ts, metric, loggerChannel) VALUES" +
                            "('2022-06-08T02:50:00.000000Z', '5', '3')," +
                            "('2022-06-08T02:50:00.000000Z', '6', '3')," +
                            "('2022-06-08T02:50:00.000000Z', '7', '1')," +
                            "('2022-06-08T02:50:00.000000Z', '8', '1')," +
                            "('2022-06-08T02:50:00.000000Z', '9', '2')," +
                            "('2022-06-08T02:50:00.000000Z', '10', '2')," +
                            "('2022-06-08T03:50:00.000000Z', '11', '2')," +
                            "('2022-06-08T03:50:00.000000Z', '12', '2')," +
                            "('2022-06-08T04:50:00.000000Z', '13', '2')," +
                            "('2022-06-08T04:50:00.000000Z', '14', '2')",
                    sqlExecutionContext).execute(null).await();

            // OOO in the middle
            compiler.compile("INSERT INTO monthly_col_top (ts, metric, sensorChannel, 'loggerChannel') VALUES" +
                            "('2022-06-08T03:30:00.000000Z', '15', '2', '3')," +
                            "('2022-06-08T03:30:00.000000Z', '16', '2', '3')",
                    sqlExecutionContext).execute(null).await();


            TestUtils.assertSql(compiler, sqlExecutionContext, "select ts, metric, loggerChannel from monthly_col_top", sink,
                    "ts\tmetric\tloggerChannel\n" +
                            "2022-06-08T01:40:00.000000Z\t1\t\n" +
                            "2022-06-08T02:41:00.000000Z\t2\t\n" +
                            "2022-06-08T02:42:00.000000Z\t3\t\n" +
                            "2022-06-08T02:43:00.000000Z\t4\t\n" +
                            "2022-06-08T02:50:00.000000Z\t5\t3\n" +
                            "2022-06-08T02:50:00.000000Z\t6\t3\n" +
                            "2022-06-08T02:50:00.000000Z\t7\t1\n" +
                            "2022-06-08T02:50:00.000000Z\t8\t1\n" +
                            "2022-06-08T02:50:00.000000Z\t9\t2\n" +
                            "2022-06-08T02:50:00.000000Z\t10\t2\n" +
                            "2022-06-08T03:30:00.000000Z\t15\t3\n" +
                            "2022-06-08T03:30:00.000000Z\t16\t3\n" +
                            "2022-06-08T03:50:00.000000Z\t11\t2\n" +
                            "2022-06-08T03:50:00.000000Z\t12\t2\n" +
                            "2022-06-08T04:50:00.000000Z\t13\t2\n" +
                            "2022-06-08T04:50:00.000000Z\t14\t2\n");

            TestUtils.assertSql(compiler, sqlExecutionContext, "select * from monthly_col_top where loggerChannel = '2'", sink,
                    "ts\tmetric\tdiagnostic\tsensorChannel\tloggerChannel\n" +
                            "2022-06-08T02:50:00.000000Z\t9\t\t\t2\n" +
                            "2022-06-08T02:50:00.000000Z\t10\t\t\t2\n" +
                            "2022-06-08T03:50:00.000000Z\t11\t\t\t2\n" +
                            "2022-06-08T03:50:00.000000Z\t12\t\t\t2\n" +
                            "2022-06-08T04:50:00.000000Z\t13\t\t\t2\n" +
                            "2022-06-08T04:50:00.000000Z\t14\t\t\t2\n");

            // OOO appends to last partition
            compiler.compile("INSERT INTO monthly_col_top (ts, metric, sensorChannel, 'loggerChannel') VALUES" +
                            "('2022-06-08T05:30:00.000000Z', '17', '4', '3')," +
                            "('2022-06-08T04:50:00.000000Z', '18', '4', '3')",
                    sqlExecutionContext).execute(null).await();

            TestUtils.assertSql(compiler, sqlExecutionContext, "select * from monthly_col_top where loggerChannel = '3'", sink,
                    "ts\tmetric\tdiagnostic\tsensorChannel\tloggerChannel\n" +
                            "2022-06-08T02:50:00.000000Z\t5\t\t\t3\n" +
                            "2022-06-08T02:50:00.000000Z\t6\t\t\t3\n" +
                            "2022-06-08T03:30:00.000000Z\t15\t\t2\t3\n" +
                            "2022-06-08T03:30:00.000000Z\t16\t\t2\t3\n" +
                            "2022-06-08T04:50:00.000000Z\t18\t\t4\t3\n" +
                            "2022-06-08T05:30:00.000000Z\t17\t\t4\t3\n");

            // OOO merges and appends to last partition
            compiler.compile("INSERT INTO monthly_col_top (ts, metric, sensorChannel, 'loggerChannel') VALUES" +
                            "('2022-06-08T05:30:00.000000Z', '19', '4', '3')," +
                            "('2022-06-08T02:50:00.000000Z', '20', '4', '3')," +
                            "('2022-06-08T02:50:00.000000Z', '21', '4', '3')",
                    sqlExecutionContext).execute(null).await();

            TestUtils.assertSql(compiler, sqlExecutionContext, "select * from monthly_col_top where loggerChannel = '3'", sink,
                    "ts\tmetric\tdiagnostic\tsensorChannel\tloggerChannel\n" +
                            "2022-06-08T02:50:00.000000Z\t5\t\t\t3\n" +
                            "2022-06-08T02:50:00.000000Z\t6\t\t\t3\n" +
                            "2022-06-08T02:50:00.000000Z\t20\t\t4\t3\n" +
                            "2022-06-08T02:50:00.000000Z\t21\t\t4\t3\n" +
                            "2022-06-08T03:30:00.000000Z\t15\t\t2\t3\n" +
                            "2022-06-08T03:30:00.000000Z\t16\t\t2\t3\n" +
                            "2022-06-08T04:50:00.000000Z\t18\t\t4\t3\n" +
                            "2022-06-08T05:30:00.000000Z\t17\t\t4\t3\n" +
                            "2022-06-08T05:30:00.000000Z\t19\t\t4\t3\n");
        });
    }

    @Test
    public void testSplitLastPartition() throws Exception {
        executeWithPool(workerCount, (engine, compiler, executionContext) -> {
            compiler.compile(
                    "create table x as (" +
                            "select" +
                            " cast(x as int) i," +
                            " -x j," +
                            " timestamp_sequence('2020-02-03T13', 60*1000000L) ts" +
                            " from long_sequence(60*24*2+300)" +
                            ") timestamp (ts) partition by DAY",
                    executionContext
            );

            compiler.compile(
                    "create table z as (" +
                            "select" +
                            " cast(x as int) * 1000000 i," +
                            " -x - 1000000L as j," +
                            " timestamp_sequence('2020-02-05T17:01', 60*1000000L) ts" +
                            " from long_sequence(50))",
                    executionContext
            );

            compiler.compile(
                    "create table y as (select * from x union all select * from z)",
                    executionContext
            );

            compiler.compile("insert into x select * from z", executionContext);

            TestUtils.assertEquals(
                    compiler,
                    executionContext,
                    "y order by ts",
                    "x"
            );
        });
    }

    @Test
    public void testSplitLastPartitionWithColumnTop() throws Exception {
        executeWithPool(workerCount,
                (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext executionContext) -> {
                    compiler.compile(
                            "create table x as (" +
                                    "select" +
                                    " cast(x as int) i," +
                                    " -x j," +
                                    " rnd_str(5,16,2) as str," +
                                    " timestamp_sequence('2020-02-03T13', 60*1000000L) ts" +
                                    " from long_sequence(60*24*2+300)" +
                                    ") timestamp (ts) partition by DAY",
                            executionContext
                    );

                    compiler.compile(
                            "create table zz (" +
                                    "i int," +
                                    "j long," +
                                    "str string," +
                                    "ts timestamp, " +
                                    "k int," +
                                    "ks string, " +
                                    "sym symbol" +
                                    ")",
                            executionContext
                    );

                    compiler.compile(
                            "create table z as (" +
                                    "select" +
                                    " cast(x as int) * 1000000 i," +
                                    " -x - 1000000L as j," +
                                    " rnd_str(5,16,2) as str," +
                                    " timestamp_sequence('2020-02-05T14:01:07', 60*100000L) ts" +
                                    " from long_sequence(100))",
                            executionContext
                    );
                    compiler.compile("insert into zz(i,j,str,ts) select i,j,str,ts from x", executionContext);
                    compiler.compile("insert into zz(i,j,str,ts) select i,j,str,ts from z", executionContext);
                    compiler.compile("insert into x(i,j,str,ts) select i,j,str,ts from z", executionContext);

                    compiler.compile("alter table x add column k int", executionContext).execute(null).await();
                    compiler.compile("alter table x add column ks string", executionContext).execute(null).await();
                    compiler.compile("alter table x add column sym symbol index ", executionContext).execute(null).await();

                    compiler.compile(
                            "create table z2 as (" +
                                    "select" +
                                    " cast(x as int) * 1000000 i," +
                                    " -x - 1000000L as j," +
                                    " rnd_str(5,16,2) as str," +
                                    " timestamp_sequence('2020-02-05T00:01:07', 60*100000L) ts," +
                                    " 1 as k," +
                                    " rnd_str(5,16,2) as ks," +
                                    " rnd_symbol(null,'5','16','2') as sym" +
                                    " from long_sequence(1000))",
                            executionContext
                    );

                    compiler.compile("insert into zz select * from z2", executionContext);
                    compiler.compile("insert into x select * from z2", executionContext);

                    assertX(compiler, executionContext, "zz");
                    assertIndex(compiler, executionContext, "zz", "x", "sym = '5'");
                    assertIndex(compiler, executionContext, "zz", "x", "sym is null");

                    // Squash last partition
                    compiler.compile("insert into zz(ts) values('2020-02-06')", executionContext).execute(null).await();
                    compiler.compile("insert into x(ts) values('2020-02-06')", executionContext).execute(null).await();

                    assertX(compiler, executionContext, "zz");
                    assertIndex(compiler, executionContext, "zz", "x", "sym = '5'");
                    assertIndex(compiler, executionContext, "zz", "x", "sym is null");
                });
    }

    @Test
    public void testSplitMidPartition() throws Exception {
        executeWithPool(workerCount,
                (
                        CairoEngine engine,
                        SqlCompiler compiler,
                        SqlExecutionContext executionContext
                ) -> {
                    compiler.compile(
                            "create table x as (" +
                                    "select" +
                                    " cast(x as int) i," +
                                    " -x j," +
                                    " rnd_str(5,16,2) as str," +
                                    " timestamp_sequence('2020-02-03T13', 60*1000000L) ts" +
                                    " from long_sequence(60*24*2)" +
                                    ") timestamp (ts) partition by DAY",
                            executionContext
                    );

                    compiler.compile(
                            "create table z as (" +
                                    "select" +
                                    " cast(x as int) * 1000000 i," +
                                    " -x - 1000000L as j," +
                                    " rnd_str(5,16,2) as str," +
                                    " timestamp_sequence('2020-02-04T23:01', 60*1000000L) ts" +
                                    " from long_sequence(50))",
                            executionContext
                    );

                    compiler.compile(
                            "create table y as (select * from x union all select * from z)",
                            executionContext
                    );

                    compiler.compile("insert into x select * from z", executionContext);

                    TestUtils.assertEquals(
                            compiler,
                            executionContext,
                            "y order by ts",
                            "x"
                    );
                });
    }

    @Test
    public void testSplitOverrunLastPartition() throws Exception {
        executeWithPool(workerCount, (
                CairoEngine engine,
                SqlCompiler compiler,
                SqlExecutionContext executionContext
        ) -> {
            compiler.compile(
                    "create table x as (" +
                            "select" +
                            " cast(x as int) i," +
                            " -x j," +
                            " rnd_str(5,16,2) as str," +
                            " timestamp_sequence('2020-02-03T13', 60*1000000L) ts" +
                            " from long_sequence(60*24*2+300)" +
                            ") timestamp (ts) partition by DAY",
                    executionContext
            );

            compiler.compile(
                    "create table z as (" +
                            "select" +
                            " cast(x as int) * 1000000 i," +
                            " -x - 1000000L as j," +
                            " rnd_str(5,16,2) as str," +
                            " timestamp_sequence('2020-02-05T17:01', 60*1000000L) ts" +
                            " from long_sequence(1000))",
                    executionContext
            );

            compiler.compile(
                    "create table y as (select * from x union all select * from z)",
                    executionContext
            );

            compiler.compile("insert into x select * from z", executionContext);

            TestUtils.assertEquals(
                    compiler,
                    executionContext,
                    "y order by ts",
                    "x"
            );
        });
    }

    @Test
    public void testSplitPartitionWithColumnTop() throws Exception {
        executeWithPool(workerCount,
                (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext executionContext) -> {
                    compiler.compile(
                            "create table x as (" +
                                    "select" +
                                    " cast(x as int) i," +
                                    " -x j," +
                                    " rnd_str(5,16,2) as str," +
                                    " timestamp_sequence('2020-02-03T13', 60*1000000L) ts" +
                                    " from long_sequence(60*24*2+300)" +
                                    ") timestamp (ts) partition by DAY",
                            executionContext
                    );
                    compiler.compile("alter table x add column k int", executionContext).execute(null).await();
                    compiler.compile("alter table x add column ks string", executionContext).execute(null).await();

                    compiler.compile(
                            "create table z as (" +
                                    "select" +
                                    " cast(x as int) * 1000000 i," +
                                    " -x - 1000000L as j," +
                                    " rnd_str(5,16,2) as str," +
                                    " timestamp_sequence('2020-02-05T17:01:05', 60*1000000L) ts," +
                                    " 1 as k," +
                                    " rnd_str(5,16,2) as ks" +
                                    " from long_sequence(1000))",
                            executionContext
                    );

                    compiler.compile(
                            "create table y as (select * from x union all select * from z)",
                            executionContext
                    );

                    compiler.compile("insert into x select * from z", executionContext);

                    assertX(compiler, executionContext, "y");
                });
    }

    @Test
    public void testSplitPartitionWithColumnTopResultsInSplitWithColumnTop() throws Exception {
        executeWithPool(workerCount,
                (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext executionContext) -> {
                    compiler.compile(
                            "create table x as (" +
                                    "select" +
                                    " cast(x as int) i," +
                                    " -x j," +
                                    " rnd_str(5,16,2) as str," +
                                    " timestamp_sequence('2020-02-03T13', 60*1000000L) ts" +
                                    " from long_sequence(60*24*2+300)" +
                                    ") timestamp (ts) partition by DAY",
                            executionContext
                    );
                    compiler.compile("alter table x add column k int", executionContext).execute(null).await();
                    compiler.compile("alter table x add column ks string", executionContext).execute(null).await();

                    compiler.compile(
                            "create table y as (" +
                                    "select" +
                                    " cast(x as int) * 1000000 i," +
                                    " -x - 1000000L as j," +
                                    " rnd_str(5,16,2) as str," +
                                    " timestamp_sequence('2020-02-05T20:01:05', 60*1000000L) ts," +
                                    " 1 as k," +
                                    " rnd_str(5,16,2) as ks" +
                                    " from long_sequence(1000))",
                            executionContext
                    );

                    compiler.compile(
                            "create table z as (" +
                                    "select" +
                                    " cast(x as int) * 1000000 i," +
                                    " -x - 1000000L as j," +
                                    " rnd_str(5,16,2) as str," +
                                    " timestamp_sequence('2020-02-05T21:01:05.2', 60*1000000L) ts," +
                                    " 1 as k," +
                                    " rnd_str(5,16,2) as ks" +
                                    " from long_sequence(1000))",
                            executionContext
                    );

                    compiler.compile(
                            "create table zz as (select * from x union all select * from y union all select * from z)",
                            executionContext
                    );

                    compiler.compile("insert into x select * from y", executionContext);
                    compiler.compile("insert into x select * from z", executionContext);

                    assertX(compiler, executionContext, "zz");
                });
    }

    @Test
    public void testSplitPartitionWithColumnTopResultsInSplitWithColumnTop2() throws Exception {
        executeWithPool(workerCount,
                (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext executionContext) -> {
                    compiler.compile(
                            "create table x as (" +
                                    "select" +
                                    " cast(x as int) i," +
                                    " -x j," +
                                    " rnd_str(5,16,2) as str," +
                                    " timestamp_sequence('2020-02-03T13', 60*1000000L) ts" +
                                    " from long_sequence(60*24*2+300)" +
                                    ") timestamp (ts) partition by DAY",
                            executionContext
                    );
                    compiler.compile("alter table x add column k int", executionContext).execute(null).await();
                    compiler.compile("alter table x add column ks string", executionContext).execute(null).await();

                    compiler.compile(
                            "create table y as (" +
                                    "select" +
                                    " cast(x as int) * 1000000 i," +
                                    " -x - 1000000L as j," +
                                    " rnd_str(5,16,2) as str," +
                                    " timestamp_sequence('2020-02-05T20:01:05', 60*1000000L) ts," +
                                    " 1 as k," +
                                    " rnd_str(5,16,2) as ks" +
                                    " from long_sequence(1000))",
                            executionContext
                    );

                    compiler.compile(
                            "create table z as (" +
                                    "select" +
                                    " cast(x as int) * 1000000 i," +
                                    " -x - 1000000L as j," +
                                    " rnd_str(5,16,2) as str," +
                                    " timestamp_sequence('2020-02-05T17:01:07', 60*1000000L) ts," +
                                    " 1 as k," +
                                    " rnd_str(5,16,2) as ks" +
                                    " from long_sequence(1000))",
                            executionContext
                    );

                    compiler.compile(
                            "create table zz as (select * from x union all select * from y union all select * from z)",
                            executionContext
                    );

                    compiler.compile("insert into x select * from y", executionContext);
                    compiler.compile("insert into x select * from z", executionContext);

                    assertX(compiler, executionContext, "zz");
                });
    }

    private static void assertX(SqlCompiler compiler, SqlExecutionContext executionContext, String expectedTable) throws SqlException {
        String limit = "";
        TestUtils.assertSqlCursors(
                compiler,
                executionContext,
                expectedTable + " order by ts" + limit,
                "x" + limit,
                LOG
        );

        TestUtils.assertEquals(
                compiler,
                executionContext,
                "select count() from " + expectedTable,
                "select count() from x"
        );

        TestUtils.assertEquals(
                compiler,
                executionContext,
                "select min(ts) from " + expectedTable,
                "select min(ts) from x"
        );

        TestUtils.assertEquals(
                compiler,
                executionContext,
                "select max(ts) from " + expectedTable,
                "select max(ts) from x"
        );
    }

    private void assertIndex(SqlCompiler compiler, SqlExecutionContext executionContext, String table1, String table2, String filter) throws SqlException {
        TestUtils.assertSqlCursors(
                compiler,
                executionContext,
                "select * from " + table1 + " where " + filter + " order by ts",
                "select * from " + table2 + " where " + filter,
                LOG
        );
    }
}