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

package io.questdb.test.cairo.o3;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CommitMode;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.FilesFacade;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.Os;
import io.questdb.std.Vect;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import static io.questdb.cairo.TableUtils.dFile;
import static io.questdb.cairo.TableUtils.iFile;
import static io.questdb.test.AbstractCairoTest.replaceTimestampSuffix1;

public class O3SplitPartitionTest extends AbstractO3Test {
    protected static final Log LOG = LogFactory.getLog(O3SplitPartitionTest.class);
    private final StringBuilder tstData = new StringBuilder();
    private final int workerCount;
    @Rule
    public TestName name = new TestName();

    public O3SplitPartitionTest() {
        this.workerCount = rnd.nextBoolean() ? 0 : 2;
        AbstractO3Test.commitMode = rnd.nextBoolean() ? CommitMode.SYNC : CommitMode.NOSYNC;
        AbstractO3Test.mixedIOEnabled = rnd.nextBoolean();
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
        executeWithPool(
                workerCount,
                (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext executionContext, String timestampTypeName) -> {
                    engine.execute(
                            "create table x as (" +
                                    "select" +
                                    " cast(x as int) i," +
                                    " -x j," +
                                    " rnd_str(5,16,2) as str," +
                                    " rnd_varchar(5,16,2) as v1," +
                                    " rnd_varchar(1,1,1) as v2," +
                                    " timestamp_sequence('2020-02-03T13', 60*1000000L)::" + timestampTypeName + " ts" +
                                    " from long_sequence(60*24*2+300)" +
                                    ") timestamp (ts) partition by DAY",
                            executionContext
                    );
                    engine.execute("alter table x add column k int", executionContext);
                    engine.execute("alter table x add column sym symbol index ", executionContext);
                    engine.execute("alter table x add column ks string", executionContext);
                    engine.execute("alter table x add column kv1 varchar", executionContext);
                    engine.execute("alter table x add column kv2 varchar", executionContext);

                    engine.execute(
                            "create table y as (" +
                                    "select" +
                                    " cast(x as int) * 1000000 i," +
                                    " -x - 1000000L as j," +
                                    " rnd_str(5,16,2) as str," +
                                    " rnd_varchar(5,16,2) as v1," +
                                    " rnd_varchar(1,1,1) as v2," +
                                    " timestamp_sequence('2020-02-05T17:01:05', 60*1000000L)::" + timestampTypeName + " ts," +
                                    " 1 as k," +
                                    " rnd_symbol(null,'5','16','2') as sym," +
                                    " rnd_str(5,16,2) as ks," +
                                    " rnd_varchar(5,16,2) as kv1," +
                                    " rnd_varchar(1,1,1) as kv2," +
                                    " from long_sequence(1000))",
                            executionContext
                    );

                    engine.execute(
                            "create table z as (" +
                                    "select" +
                                    " cast(x as int) * 1000000 i," +
                                    " -x - 1000000L as j," +
                                    " rnd_str(5,16,2) as str," +
                                    " rnd_varchar(5,16,2) as v1," +
                                    " rnd_varchar(1,1,1) as v2," +
                                    " timestamp_sequence('2020-02-05T14:01:07', 60*1000000L)::" + timestampTypeName + " ts," +
                                    " 1 as k," +
                                    " rnd_symbol(null,'5','16','2') as sym," +
                                    " rnd_str(5,16,2) as ks," +
                                    " rnd_varchar(5,16,2) as kv1," +
                                    " rnd_varchar(1,1,1) as kv2," +
                                    " from long_sequence(1000))",
                            executionContext
                    );

                    engine.execute(
                            "create table zz (" +
                                    "i int," +
                                    "j long," +
                                    "str string," +
                                    "v1 varchar," +
                                    "v2 varchar," +
                                    "ts " + timestampTypeName + ", " +
                                    "k int," +
                                    "sym symbol," +
                                    "ks string," +
                                    "kv1 varchar," +
                                    "kv2 varchar" +
                                    ")",
                            executionContext
                    );

                    engine.execute(
                            "insert into zz select * from x union all select * from y union all select * from z",
                            executionContext
                    );

                    engine.execute("insert into x select * from y", executionContext);
                    engine.execute("insert into x select * from z", executionContext);

                    assertX(compiler, executionContext, "zz");
                    assertIndex(compiler, executionContext, "sym = '5'");
                    assertIndex(compiler, executionContext, "sym is null");
                }
        );
    }

    @Test
    public void testRebuildIndexLastPartitionWithColumnTop() throws Exception {
        executeWithPool(
                workerCount, (engine, compiler, sqlExecutionContext, timestampTypeName) -> {
                    partitionO3SplitThreshold = 5;

                    engine.execute(
                            "CREATE TABLE monthly_col_top(" +
                                    "ts " + timestampTypeName + ", metric SYMBOL, diagnostic SYMBOL, sensorChannel SYMBOL" +
                                    ") timestamp(ts) partition by MONTH",
                            sqlExecutionContext
                    );

                    engine.execute(
                            "INSERT INTO monthly_col_top (ts, metric, diagnostic, sensorChannel) VALUES" +
                                    "('2022-06-08T01:40:00.000000Z', '1', 'true', '2')," +
                                    "('2022-06-08T02:41:00.000000Z', '2', 'true', '2')," +
                                    "('2022-06-08T02:42:00.000000Z', '3', 'true', '1')," +
                                    "('2022-06-08T02:43:00.000000Z', '4', 'true', '1')", sqlExecutionContext
                    );

                    engine.execute("ALTER TABLE monthly_col_top ADD COLUMN loggerChannel SYMBOL INDEX", sqlExecutionContext);

                    engine.execute(
                            "INSERT INTO monthly_col_top (ts, metric, loggerChannel) VALUES" +
                                    "('2022-06-08T02:50:00.000000Z', '5', '3')," +
                                    "('2022-06-08T02:50:00.000000Z', '6', '3')," +
                                    "('2022-06-08T02:50:00.000000Z', '7', '1')," +
                                    "('2022-06-08T02:50:00.000000Z', '8', '1')," +
                                    "('2022-06-08T02:50:00.000000Z', '9', '2')," +
                                    "('2022-06-08T02:50:00.000000Z', '10', '2')," +
                                    "('2022-06-08T03:50:00.000000Z', '11', '2')," +
                                    "('2022-06-08T03:50:00.000000Z', '12', '2')," +
                                    "('2022-06-08T04:50:00.000000Z', '13', '2')," +
                                    "('2022-06-08T04:50:00.000000Z', '14', '2')", sqlExecutionContext
                    );

                    // OOO in the middle
                    engine.execute(
                            "INSERT INTO monthly_col_top (ts, metric, sensorChannel, 'loggerChannel') VALUES" +
                                    "('2022-06-08T03:30:00.000000Z', '15', '2', '3')," +
                                    "('2022-06-08T03:30:00.000000Z', '16', '2', '3')", sqlExecutionContext
                    );


                    TestUtils.assertSql(
                            compiler, sqlExecutionContext, "select ts, metric, loggerChannel from monthly_col_top", sink,
                            replaceTimestampSuffix1("""
                                    ts\tmetric\tloggerChannel
                                    2022-06-08T01:40:00.000000Z\t1\t
                                    2022-06-08T02:41:00.000000Z\t2\t
                                    2022-06-08T02:42:00.000000Z\t3\t
                                    2022-06-08T02:43:00.000000Z\t4\t
                                    2022-06-08T02:50:00.000000Z\t5\t3
                                    2022-06-08T02:50:00.000000Z\t6\t3
                                    2022-06-08T02:50:00.000000Z\t7\t1
                                    2022-06-08T02:50:00.000000Z\t8\t1
                                    2022-06-08T02:50:00.000000Z\t9\t2
                                    2022-06-08T02:50:00.000000Z\t10\t2
                                    2022-06-08T03:30:00.000000Z\t15\t3
                                    2022-06-08T03:30:00.000000Z\t16\t3
                                    2022-06-08T03:50:00.000000Z\t11\t2
                                    2022-06-08T03:50:00.000000Z\t12\t2
                                    2022-06-08T04:50:00.000000Z\t13\t2
                                    2022-06-08T04:50:00.000000Z\t14\t2
                                    """, timestampTypeName)
                    );

                    TestUtils.assertSql(
                            compiler, sqlExecutionContext, "select * from monthly_col_top where loggerChannel = '2'", sink,
                            replaceTimestampSuffix1("""
                                    ts\tmetric\tdiagnostic\tsensorChannel\tloggerChannel
                                    2022-06-08T02:50:00.000000Z\t9\t\t\t2
                                    2022-06-08T02:50:00.000000Z\t10\t\t\t2
                                    2022-06-08T03:50:00.000000Z\t11\t\t\t2
                                    2022-06-08T03:50:00.000000Z\t12\t\t\t2
                                    2022-06-08T04:50:00.000000Z\t13\t\t\t2
                                    2022-06-08T04:50:00.000000Z\t14\t\t\t2
                                    """, timestampTypeName)
                    );

                    // OOO appends to last partition
                    engine.execute(
                            "INSERT INTO monthly_col_top (ts, metric, sensorChannel, 'loggerChannel') VALUES" +
                                    "('2022-06-08T05:30:00.000000Z', '17', '4', '3')," +
                                    "('2022-06-08T04:50:00.000000Z', '18', '4', '3')", sqlExecutionContext
                    );

                    TestUtils.assertSql(
                            compiler, sqlExecutionContext, "select * from monthly_col_top where loggerChannel = '3'", sink,
                            replaceTimestampSuffix1("""
                                    ts\tmetric\tdiagnostic\tsensorChannel\tloggerChannel
                                    2022-06-08T02:50:00.000000Z\t5\t\t\t3
                                    2022-06-08T02:50:00.000000Z\t6\t\t\t3
                                    2022-06-08T03:30:00.000000Z\t15\t\t2\t3
                                    2022-06-08T03:30:00.000000Z\t16\t\t2\t3
                                    2022-06-08T04:50:00.000000Z\t18\t\t4\t3
                                    2022-06-08T05:30:00.000000Z\t17\t\t4\t3
                                    """, timestampTypeName)
                    );

                    // OOO merges and appends to last partition
                    engine.execute(
                            "INSERT INTO monthly_col_top (ts, metric, sensorChannel, 'loggerChannel') VALUES" +
                                    "('2022-06-08T05:30:00.000000Z', '19', '4', '3')," +
                                    "('2022-06-08T02:50:00.000000Z', '20', '4', '3')," +
                                    "('2022-06-08T02:50:00.000000Z', '21', '4', '3')", sqlExecutionContext
                    );

                    TestUtils.assertSql(
                            compiler, sqlExecutionContext, "select * from monthly_col_top where loggerChannel = '3'", sink,
                            replaceTimestampSuffix1("""
                                    ts\tmetric\tdiagnostic\tsensorChannel\tloggerChannel
                                    2022-06-08T02:50:00.000000Z\t5\t\t\t3
                                    2022-06-08T02:50:00.000000Z\t6\t\t\t3
                                    2022-06-08T02:50:00.000000Z\t20\t\t4\t3
                                    2022-06-08T02:50:00.000000Z\t21\t\t4\t3
                                    2022-06-08T03:30:00.000000Z\t15\t\t2\t3
                                    2022-06-08T03:30:00.000000Z\t16\t\t2\t3
                                    2022-06-08T04:50:00.000000Z\t18\t\t4\t3
                                    2022-06-08T05:30:00.000000Z\t17\t\t4\t3
                                    2022-06-08T05:30:00.000000Z\t19\t\t4\t3
                                    """, timestampTypeName)
                    );
                }
        );
    }

    @Test
    public void testSplitLastPartition() throws Exception {
        executeWithPool(
                workerCount, (engine, compiler, executionContext, timestampTypeName) -> {
                    engine.execute(
                            "create table x as (" +
                                    "select" +
                                    " cast(x as int) i," +
                                    " -x j," +
                                    " timestamp_sequence('2020-02-03T13', 60*1000000L)::" + timestampTypeName + " ts" +
                                    " from long_sequence(60*24*2+300)" +
                                    ") timestamp (ts) partition by DAY",
                            executionContext
                    );

                    engine.execute(
                            "create table z as (" +
                                    "select" +
                                    " cast(x as int) * 1000000 i," +
                                    " -x - 1000000L as j," +
                                    " timestamp_sequence('2020-02-05T17:01', 60*1000000L)::" + timestampTypeName + " ts" +
                                    " from long_sequence(50))",
                            executionContext
                    );

                    engine.execute(
                            "create table y as (select * from x union all select * from z)",
                            executionContext
                    );

                    engine.execute("insert into x select * from z", executionContext);

                    TestUtils.assertEquals(
                            compiler,
                            executionContext,
                            "y order by ts",
                            "x"
                    );
                }
        );
    }

    @Test
    public void testSplitLastPartitionWithColumnTop() throws Exception {
        executeWithPool(
                workerCount,
                (engine, compiler, executionContext, timestampTypeName) -> {
                    engine.execute(
                            "create table x as (" +
                                    "select" +
                                    " cast(x as int) i," +
                                    " -x j," +
                                    " rnd_str(5,16,2) as str," +
                                    " rnd_varchar(5,16,2) as v1," +
                                    " rnd_varchar(1,1,1) as v2," +
                                    " timestamp_sequence('2020-02-03T13', 60*1000000L)::" + timestampTypeName + " ts" +
                                    " from long_sequence(60*24*2+300)" +
                                    ") timestamp (ts) partition by DAY",
                            executionContext
                    );

                    engine.execute(
                            "create table zz (" +
                                    "i int," +
                                    "j long," +
                                    "str string," +
                                    "v1 varchar," +
                                    "v2 varchar," +
                                    "ts " + timestampTypeName + ", " +
                                    "k int," +
                                    "ks string, " +
                                    "kv1 varchar, " +
                                    "kv2 varchar, " +
                                    "sym symbol" +
                                    ")",
                            executionContext
                    );

                    engine.execute(
                            "create table z as (" +
                                    "select" +
                                    " cast(x as int) * 1000000 i," +
                                    " -x - 1000000L as j," +
                                    " rnd_str(5,16,2) as str," +
                                    " rnd_varchar(5,16,2) as v1," +
                                    " rnd_varchar(1,1,1) as v2," +
                                    " timestamp_sequence('2020-02-05T14:01:07', 60*100000L)::" + timestampTypeName + " ts" +
                                    " from long_sequence(100))",
                            executionContext
                    );
                    engine.execute("insert into zz(i,j,str,v1,v2,ts) select i,j,str,v1,v2,ts from x", executionContext);
                    engine.execute("insert into zz(i,j,str,v1,v2,ts) select i,j,str,v1,v2,ts from z", executionContext);
                    engine.execute("insert into x(i,j,str,v1,v2,ts) select i,j,str,v1,v2,ts from z", executionContext);

                    engine.execute("alter table x add column k int", executionContext);
                    engine.execute("alter table x add column ks string", executionContext);
                    engine.execute("alter table x add column kv1 varchar", executionContext);
                    engine.execute("alter table x add column kv2 varchar", executionContext);
                    engine.execute("alter table x add column sym symbol index ", executionContext);

                    engine.execute(
                            "create table z2 as (" +
                                    "select" +
                                    " cast(x as int) * 1000000 i," +
                                    " -x - 1000000L as j," +
                                    " rnd_str(5,16,2) as str," +
                                    " rnd_varchar(5,16,2) as v1," +
                                    " rnd_varchar(1,1,1) as v2," +
                                    " timestamp_sequence('2020-02-05T00:01:07', 60*100000L)::" + timestampTypeName + " ts," +
                                    " 1 as k," +
                                    " rnd_str(5,16,2) as ks," +
                                    " rnd_varchar(5,16,2) as kv1," +
                                    " rnd_varchar(1,1,1) as kv2," +
                                    " rnd_symbol(null,'5','16','2') as sym" +
                                    " from long_sequence(1000))",
                            executionContext
                    );

                    engine.execute("insert into zz select * from z2", executionContext);
                    engine.execute("insert into x select * from z2", executionContext);

                    assertX(compiler, executionContext, "zz");
                    assertIndex(compiler, executionContext, "sym = '5'");
                    assertIndex(compiler, executionContext, "sym is null");

                    // Squash last partition
                    engine.execute("insert into zz(ts) values('2020-02-06')", executionContext);
                    engine.execute("insert into x(ts) values('2020-02-06')", executionContext);

                    assertX(compiler, executionContext, "zz");
                    assertIndex(compiler, executionContext, "sym = '5'");
                    assertIndex(compiler, executionContext, "sym is null");
                }
        );
    }

    @Test
    public void testSplitMidPartition() throws Exception {
        executeWithPool(
                workerCount,
                (engine, compiler, executionContext, timestampTypeName) -> {
                    engine.execute(
                            "create table x as (" +
                                    "select" +
                                    " cast(x as int) i," +
                                    " -x j," +
                                    " rnd_str(5,16,2) as str," +
                                    " rnd_varchar(5,16,2) as v1," +
                                    " rnd_varchar(1,1,1) as v2," +
                                    " timestamp_sequence('2020-02-03T13', 60*1000000L)::" + timestampTypeName + " ts" +
                                    " from long_sequence(60*24*2)" +
                                    ") timestamp (ts) partition by DAY",
                            executionContext
                    );

                    engine.execute(
                            "create table z as (" +
                                    "select" +
                                    " cast(x as int) * 1000000 i," +
                                    " -x - 1000000L as j," +
                                    " rnd_str(5,16,2) as str," +
                                    " rnd_varchar(5,16,2) as v1," +
                                    " rnd_varchar(1,1,1) as v2," +
                                    " timestamp_sequence('2020-02-04T23:01', 60*1000000L)::" + timestampTypeName + " ts" +
                                    " from long_sequence(50))",
                            executionContext
                    );

                    engine.execute(
                            "create table y as (select * from x union all select * from z)",
                            executionContext
                    );

                    engine.execute("insert into x select * from z", executionContext);

                    TestUtils.assertEquals(
                            compiler,
                            executionContext,
                            "y order by ts",
                            "x"
                    );
                }
        );
    }

    @Test
    public void testSplitMidPartitionAfterStringColumnUpdate() throws Exception {
        executeWithPool(
                workerCount,
                (engine, compiler, executionContext, timestampTypeName) -> {
                    FilesFacade ff = FilesFacadeImpl.INSTANCE;
                    engine.execute(
                            "create table x as (" +
                                    "select" +
                                    " cast(x as int) i," +
                                    " -x j," +
                                    " rnd_str(5,16,2) as str," +
                                    " timestamp_sequence('2020-02-03T13', 60*1000000L)::" + timestampTypeName + " ts" +
                                    " from long_sequence(60*24*2)" +
                                    ") timestamp (ts) partition by DAY",
                            executionContext
                    );

                    engine.update("update x set str = str where ts >= '2020-02-04'", executionContext);

                    LPSZ colVer = dFile(Path.getThreadLocal(engine.getConfiguration().getDbRoot()).concat(engine.verifyTableName("x")).concat("2020-02-04"), "str", -1);
                    LOG.info().$("deleting ").$(colVer).$();
                    Assert.assertTrue(Os.isWindows() || ff.removeQuiet(colVer));

                    colVer = iFile(Path.getThreadLocal(engine.getConfiguration().getDbRoot()).concat(engine.verifyTableName("x")).concat("2020-02-04"), "str", -1);
                    LOG.info().$("deleting ").$(colVer).$();
                    Assert.assertTrue(Os.isWindows() || ff.removeQuiet(colVer));

                    engine.releaseInactive();

                    engine.execute(
                            "create table z as (" +
                                    "select" +
                                    " cast(x as int) * 1000000 i," +
                                    " -x - 1000000L as j," +
                                    " rnd_str(5,16,2) as str," +
                                    " timestamp_sequence('2020-02-04T23:01', 60*1000000L)::" + timestampTypeName + " ts" +
                                    " from long_sequence(50))",
                            executionContext
                    );

                    engine.execute(
                            "create table y as (select * from x union all select * from z)",
                            executionContext
                    );

                    engine.execute("insert into x select * from z", executionContext);

                    TestUtils.assertEquals(
                            compiler,
                            executionContext,
                            "y order by ts",
                            "x"
                    );
                }
        );
    }

    @Test
    public void testSplitMidPartitionAfterVarcharColumnUpdate() throws Exception {
        executeWithPool(
                workerCount,
                (engine, compiler, executionContext, timestampTypeName) -> {
                    FilesFacade ff = FilesFacadeImpl.INSTANCE;
                    engine.execute(
                            "create table x as (" +
                                    "select" +
                                    " cast(x as int) i," +
                                    " -x j," +
                                    " rnd_varchar(5,16,2) as v1," +
                                    " rnd_varchar(1,1,1) as v2," +
                                    " timestamp_sequence('2020-02-03T13', 60*1000000L)::" + timestampTypeName + " ts" +
                                    " from long_sequence(60*24*2)" +
                                    ") timestamp (ts) partition by DAY",
                            executionContext
                    );

                    engine.update("update x set v1 = v1 where ts >= '2020-02-04'", executionContext);

                    LPSZ colVer1 = dFile(Path.getThreadLocal(engine.getConfiguration().getDbRoot()).concat(engine.verifyTableName("x")).concat("2020-02-04"), "v1", -1);
                    LOG.info().$("deleting ").$(colVer1).$();
                    Assert.assertTrue(Os.isWindows() || ff.removeQuiet(colVer1));

                    colVer1 = iFile(Path.getThreadLocal(engine.getConfiguration().getDbRoot()).concat(engine.verifyTableName("x")).concat("2020-02-04"), "v1", -1);
                    LOG.info().$("deleting ").$(colVer1).$();
                    Assert.assertTrue(Os.isWindows() || ff.removeQuiet(colVer1));

                    engine.update("update x set v2 = v2 where ts >= '2020-02-04'", executionContext);

                    LPSZ colVer2 = dFile(Path.getThreadLocal(engine.getConfiguration().getDbRoot()).concat(engine.verifyTableName("x")).concat("2020-02-04"), "v2", -1);
                    LOG.info().$("deleting ").$(colVer2).$();
                    Assert.assertTrue(Os.isWindows() || ff.removeQuiet(colVer2));

                    colVer2 = iFile(Path.getThreadLocal(engine.getConfiguration().getDbRoot()).concat(engine.verifyTableName("x")).concat("2020-02-04"), "v2", -1);
                    LOG.info().$("deleting ").$(colVer2).$();
                    Assert.assertTrue(Os.isWindows() || ff.removeQuiet(colVer2));

                    engine.releaseInactive();

                    engine.execute(
                            "create table z as (" +
                                    "select" +
                                    " cast(x as int) * 1000000 i," +
                                    " -x - 1000000L as j," +
                                    " rnd_varchar(5,16,2) as v1," +
                                    " rnd_varchar(1,1,1) as v2," +
                                    " timestamp_sequence('2020-02-04T23:01', 60*1000000L)::" + timestampTypeName + " ts" +
                                    " from long_sequence(50))",
                            executionContext
                    );

                    engine.execute(
                            "create table y as (select * from x union all select * from z)",
                            executionContext
                    );

                    engine.execute("insert into x select * from z", executionContext);

                    TestUtils.assertEquals(
                            compiler,
                            executionContext,
                            "y order by ts",
                            "x"
                    );
                }
        );
    }

    @Test
    public void testSplitOverrunLastPartition() throws Exception {
        executeWithPool(
                workerCount,
                (engine, compiler, executionContext, timestampTypeName) -> {
                    engine.execute(
                            "create table x as (" +
                                    "select" +
                                    " cast(x as int) i," +
                                    " -x j," +
                                    " rnd_str(5,16,2) as str," +
                                    " rnd_varchar(5,16,2) as v1," +
                                    " rnd_varchar(1,1,1) as v2," +
                                    " timestamp_sequence('2020-02-03T13', 60*1000000L)::" + timestampTypeName + " ts" +
                                    " from long_sequence(60*24*2+300)" +
                                    ") timestamp (ts) partition by DAY",
                            executionContext
                    );

                    engine.execute(
                            "create table z as (" +
                                    "select" +
                                    " cast(x as int) * 1000000 i," +
                                    " -x - 1000000L as j," +
                                    " rnd_str(5,16,2) as str," +
                                    " rnd_varchar(5,16,2) as v1," +
                                    " rnd_varchar(1,1,1) as v2," +
                                    " timestamp_sequence('2020-02-05T17:01', 60*1000000L)::" + timestampTypeName + " ts" +
                                    " from long_sequence(1000))",
                            executionContext
                    );

                    engine.execute(
                            "create table y as (select * from x union all select * from z)",
                            executionContext
                    );

                    engine.execute("insert into x select * from z", executionContext);

                    TestUtils.assertEquals(
                            compiler,
                            executionContext,
                            "y order by ts",
                            "x"
                    );
                }
        );
    }

    @Test
    public void testSplitPartitionWithColumnTop() throws Exception {
        executeWithPool(
                workerCount,
                (engine, compiler, executionContext, timestampTypeName) -> {
                    engine.execute(
                            "create table x as (" +
                                    "select" +
                                    " cast(x as int) i," +
                                    " -x j," +
                                    " rnd_str(5,16,2) as str," +
                                    " rnd_varchar(5,16,2) as v1," +
                                    " rnd_varchar(1,1,1) as v2," +
                                    " timestamp_sequence('2020-02-03T13', 60*1000000L)::" + timestampTypeName + " ts" +
                                    " from long_sequence(60*24*2+300)" +
                                    ") timestamp (ts) partition by DAY",
                            executionContext
                    );
                    engine.execute("alter table x add column k int", executionContext);
                    engine.execute("alter table x add column ks string", executionContext);
                    engine.execute("alter table x add column kv1 varchar", executionContext);
                    engine.execute("alter table x add column kv2 varchar", executionContext);

                    engine.execute(
                            "create table z as (" +
                                    "select" +
                                    " cast(x as int) * 1000000 i," +
                                    " -x - 1000000L as j," +
                                    " rnd_str(5,16,2) as str," +
                                    " rnd_varchar(5,16,2) as v1," +
                                    " rnd_varchar(1,1,1) as v2," +
                                    " timestamp_sequence('2020-02-05T17:01:05', 60*1000000L)::" + timestampTypeName + " ts," +
                                    " 1 as k," +
                                    " rnd_str(5,16,2) as ks," +
                                    " rnd_varchar(5,16,2) as kv1," +
                                    " rnd_varchar(1,1,1) as kv2," +
                                    " from long_sequence(1000))",
                            executionContext
                    );

                    engine.execute(
                            "create table y as (select * from x union all select * from z)",
                            executionContext
                    );

                    engine.execute("insert into x select * from z", executionContext);

                    assertX(compiler, executionContext, "y");
                }
        );
    }

    @Test
    public void testSplitPartitionWithColumnTopResultsInSplitWithColumnTop() throws Exception {
        executeWithPool(
                workerCount,
                (engine, compiler, executionContext, timestampTypeName) -> {
                    engine.execute(
                            "create table x as (" +
                                    "select" +
                                    " cast(x as int) i," +
                                    " -x j," +
                                    " rnd_str(5,16,2) as str," +
                                    " rnd_varchar(5,16,2) as v1," +
                                    " rnd_varchar(1,1,1) as v2," +
                                    " timestamp_sequence('2020-02-03T13', 60*1000000L)::" + timestampTypeName + " ts" +
                                    " from long_sequence(60*24*2+300)" +
                                    ") timestamp (ts) partition by DAY",
                            executionContext
                    );
                    engine.execute("alter table x add column k int", executionContext);
                    engine.execute("alter table x add column ks string", executionContext);
                    engine.execute("alter table x add column kv1 varchar", executionContext);
                    engine.execute("alter table x add column kv2 varchar", executionContext);

                    engine.execute(
                            "create table y as (" +
                                    "select" +
                                    " cast(x as int) * 1000000 i," +
                                    " -x - 1000000L as j," +
                                    " rnd_str(5,16,2) as str," +
                                    " rnd_varchar(5,16,2) as v1," +
                                    " rnd_varchar(1,1,1) as v2," +
                                    " timestamp_sequence('2020-02-05T20:01:05', 60*1000000L)::" + timestampTypeName + " ts," +
                                    " 1 as k," +
                                    " rnd_str(5,16,2) as ks," +
                                    " rnd_varchar(5,16,2) as kv1," +
                                    " rnd_varchar(1,1,1) as kv2," +
                                    " from long_sequence(1000))",
                            executionContext
                    );

                    engine.execute(
                            "create table z as (" +
                                    "select" +
                                    " cast(x as int) * 1000000 i," +
                                    " -x - 1000000L as j," +
                                    " rnd_str(5,16,2) as str," +
                                    " rnd_varchar(5,16,2) as v1," +
                                    " rnd_varchar(1,1,1) as v2," +
                                    " timestamp_sequence('2020-02-05T21:01:05.2', 60*1000000L)::" + timestampTypeName + " ts," +
                                    " 1 as k," +
                                    " rnd_str(5,16,2) as ks," +
                                    " rnd_varchar(5,16,2) as kv," +
                                    " rnd_varchar(1,1,1) as kv2," +
                                    " from long_sequence(1000))",
                            executionContext
                    );

                    engine.execute(
                            "create table zz as (select * from x union all select * from y union all select * from z)",
                            executionContext
                    );

                    engine.execute("insert into x select * from y", executionContext);
                    engine.execute("insert into x select * from z", executionContext);

                    assertX(compiler, executionContext, "zz");
                }
        );
    }

    @Test
    public void testSplitPartitionWithColumnTopResultsInSplitWithColumnTop2() throws Exception {
        executeWithPool(
                workerCount,
                (engine, compiler, executionContext, timestampTypeName) -> {
                    engine.execute(
                            "create table x as (" +
                                    "select" +
                                    " cast(x as int) i," +
                                    " -x j," +
                                    " rnd_str(5,16,2) as str," +
                                    " rnd_varchar(5,16,2) as v1," +
                                    " rnd_varchar(1,1,1) as v2," +
                                    " timestamp_sequence('2020-02-03T13', 60*1000000L)::" + timestampTypeName + " ts" +
                                    " from long_sequence(60*24*2+300)" +
                                    ") timestamp (ts) partition by DAY",
                            executionContext
                    );
                    engine.execute("alter table x add column k int", executionContext);
                    engine.execute("alter table x add column ks string", executionContext);
                    engine.execute("alter table x add column kv1 varchar", executionContext);
                    engine.execute("alter table x add column kv2 varchar", executionContext);

                    engine.execute(
                            "create table y as (" +
                                    "select" +
                                    " cast(x as int) * 1000000 i," +
                                    " -x - 1000000L as j," +
                                    " rnd_str(5,16,2) as str," +
                                    " rnd_varchar(5,16,2) as v1," +
                                    " rnd_varchar(1,1,1) as v2," +
                                    " timestamp_sequence('2020-02-05T20:01:05', 60*1000000L)::" + timestampTypeName + " ts," +
                                    " 1 as k," +
                                    " rnd_str(5,16,2) as ks," +
                                    " rnd_varchar(5,16,2) as kv1," +
                                    " rnd_varchar(1,1,1) as kv2," +
                                    " from long_sequence(1000))",
                            executionContext
                    );

                    engine.execute(
                            "create table z as (" +
                                    "select" +
                                    " cast(x as int) * 1000000 i," +
                                    " -x - 1000000L as j," +
                                    " rnd_str(5,16,2) as str," +
                                    " rnd_varchar(5,16,2) as v1," +
                                    " rnd_varchar(1,1,1) as v2," +
                                    " timestamp_sequence('2020-02-05T17:01:07', 60*1000000L)::" + timestampTypeName + " ts," +
                                    " 1 as k," +
                                    " rnd_str(5,16,2) as ks," +
                                    " rnd_varchar(5,16,2) as kv1," +
                                    " rnd_varchar(1,1,1) as kv2," +
                                    " from long_sequence(1000))",
                            executionContext
                    );

                    engine.execute(
                            "create table zz as (select * from x union all select * from y union all select * from z)",
                            executionContext
                    );

                    engine.execute("insert into x select * from y", executionContext);
                    engine.execute("insert into x select * from z", executionContext);

                    assertX(compiler, executionContext, "zz");
                }
        );
    }

    @Test
    public void testSplitSquashMidPartitionWithDedupSameRowCount() throws Exception {
        executeWithPool(
                workerCount,
                (engine, compiler, executionContext, timestampTypeName) -> {
                    engine.execute(
                            "create table x as (" +
                                    "select" +
                                    " cast(x as int) i," +
                                    " -x j," +
                                    " rnd_str(5,16,2) as str," +
                                    " rnd_varchar(5,16,2) as v1," +
                                    " rnd_varchar(1,1,1) as v2," +
                                    " cast(null as string) str2," +
                                    " timestamp_sequence('2020-02-03T13', 60*1000000L)::" + timestampTypeName + " ts" +
                                    " from long_sequence(60*24*2)" +
                                    ") timestamp (ts) partition by DAY WAL dedup upsert keys(ts)",
                            executionContext
                    );

                    drainWalQueue(engine);

                    // Open reader
                    TestUtils.assertSql(
                            engine, executionContext, "select sum(j), ts, last(str2) from x sample by 1d", sink,
                            replaceTimestampSuffix1("""
                                    sum\tts\tlast
                                    -218130\t2020-02-03T00:00:00.000000Z\t
                                    -1987920\t2020-02-04T00:00:00.000000Z\t
                                    -1942590\t2020-02-05T00:00:00.000000Z\t
                                    """, timestampTypeName)
                    );

                    engine.execute(
                            "create table z as (" +
                                    "select" +
                                    " cast(x as int) * 1000000 i," +
                                    " -x - 1000000L as j," +
                                    " rnd_str(5,16,2) as str," +
                                    " rnd_varchar(5,16,2) as v1," +
                                    " rnd_varchar(1,1,1) as v2," +
                                    " rnd_str(1000, 1000, 0) as str2, " +
                                    " timestamp_sequence('2020-02-04T23:01', 60*1000000L)::" + timestampTypeName + " ts" +
                                    " from long_sequence(50))",
                            executionContext
                    );

                    engine.execute("insert into x select * from z", executionContext);

                    drainWalQueue(engine);

                    TestUtils.assertSql(
                            engine, executionContext, "select sum(j), ts, last(str2) from x sample by 1d", sink,
                            replaceTimestampSuffix1("""
                                    sum\tts\tlast
                                    -218130\t2020-02-03T00:00:00.000000Z\t
                                    -51885870\t2020-02-04T00:00:00.000000Z\t
                                    -1942590\t2020-02-05T00:00:00.000000Z\t
                                    """, timestampTypeName)
                    );
                }
        );
    }

    @Test
    public void testSplitSquashMidPartitionWithDedupSameRowCountVarchar() throws Exception {
        executeWithPool(
                workerCount,
                (engine, compiler, executionContext, timestampTypeName) -> {
                    engine.execute(
                            "create table x as (" +
                                    "select" +
                                    " cast(x as int) i," +
                                    " -x j," +
                                    " rnd_str(5,16,2) as str," +
                                    " rnd_varchar(5,16,2) as v1," +
                                    " rnd_varchar(1,1,1) as v2," +
                                    " cast(null as varchar) str2," +
                                    " timestamp_sequence('2020-02-03T13', 60*1000000L)::" + timestampTypeName + " ts" +
                                    " from long_sequence(60*24*2)" +
                                    ") timestamp (ts) partition by DAY WAL dedup upsert keys(ts)",
                            executionContext
                    );

                    drainWalQueue(engine);

                    // Open reader
                    TestUtils.assertSql(
                            engine, executionContext, "select sum(j), ts, last(str2) from x sample by 1d", sink,
                            replaceTimestampSuffix1("""
                                    sum\tts\tlast
                                    -218130\t2020-02-03T00:00:00.000000Z\t
                                    -1987920\t2020-02-04T00:00:00.000000Z\t
                                    -1942590\t2020-02-05T00:00:00.000000Z\t
                                    """, timestampTypeName)
                    );

                    engine.execute(
                            "create table z as (" +
                                    "select" +
                                    " cast(x as int) * 1000000 i," +
                                    " -x - 1000000L as j," +
                                    " rnd_str(5,16,2) as str," +
                                    " rnd_varchar(5,16,2) as v1," +
                                    " rnd_varchar(1,1,1) as v2," +
                                    " rnd_varchar(1000, 1000, 0) as str2, " +
                                    " timestamp_sequence('2020-02-04T23:01', 60*1000000L)::" + timestampTypeName + " ts" +
                                    " from long_sequence(50))",
                            executionContext
                    );

                    engine.execute("insert into x select * from z", executionContext);

                    drainWalQueue(engine);

                    TestUtils.assertSql(
                            engine, executionContext, "select sum(j), ts, last(str2) from x sample by 1d", sink,
                            replaceTimestampSuffix1("""
                                    sum\tts\tlast
                                    -218130\t2020-02-03T00:00:00.000000Z\t
                                    -51885870\t2020-02-04T00:00:00.000000Z\t
                                    -1942590\t2020-02-05T00:00:00.000000Z\t
                                    """, timestampTypeName)
                    );
                }
        );
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

    private void assertIndex(SqlCompiler compiler, SqlExecutionContext executionContext, String filter) throws SqlException {
        TestUtils.assertSqlCursors(
                compiler,
                executionContext,
                "select * from " + "zz" + " where " + filter + " order by ts",
                "select * from " + "x" + " where " + filter,
                LOG
        );
    }
}