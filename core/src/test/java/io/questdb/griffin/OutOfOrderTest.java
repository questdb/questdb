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

import io.questdb.WorkerPoolAwareConfiguration;
import io.questdb.cairo.*;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.engine.functions.rnd.SharedRandom;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.std.Chars;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.DirectCharSink;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;

public class OutOfOrderTest extends AbstractGriffinTest {

    protected static final StringSink sink2 = new StringSink();
    private final static Log LOG = LogFactory.getLog(OutOfOrderTest.class);

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

        // instantiate these paths so that they are not included in memory leak test
        Path.PATH.get();
        Path.PATH2.get();
    }

    @Test
    public void testBench() throws Exception {
        executeVanilla(() -> testBench0(
                engine,
                compiler,
                sqlExecutionContext
        ));
    }

    @Test
    public void testBench2Contended() throws Exception {
        executeWithPool(0, this::bench20);
    }

    @Test
    public void testBench2Parallel() throws Exception {
        executeWithPool(8, this::bench20);
    }

    @Test
    public void testBenchContended() throws Exception {
        executeWithPool(0, this::testBench0);
    }

    @Test
    public void testBenchParallel() throws Exception {
        executeWithPool(4, this::testBench0);
    }

    @Test
    public void testColumnTopLastAppend() throws Exception {
        executeVanilla(() -> testColumnTopLastAppendColumn0(
                engine,
                compiler,
                sqlExecutionContext
        ));
    }

    @Test
    public void testColumnTopLastAppendBlankContended() throws Exception {
        executeWithPool(0, OutOfOrderTest::testColumnTopLastAppendBlankColumn0);
    }

    @Test
    public void testColumnTopLastAppendContended() throws Exception {
        executeWithPool(0, OutOfOrderTest::testColumnTopLastAppendColumn0);
    }

    @Test
    public void testColumnTopLastDataMerge() throws Exception {
        executeVanilla(() -> testColumnTopLastDataMergeData0(
                engine,
                compiler,
                sqlExecutionContext
        ));
    }

    @Test
    public void testColumnTopLastDataMerge2Data() throws Exception {
        executeVanilla(() -> testColumnTopLastDataMerge2Data0(
                engine,
                compiler,
                sqlExecutionContext
        ));
    }

    @Test
    public void testColumnTopLastDataMerge2DataContended() throws Exception {
        executeWithPool(0, OutOfOrderTest::testColumnTopLastDataMerge2Data0);
    }

    @Test
    public void testColumnTopLastDataMerge2DataParallel() throws Exception {
        executeWithPool(4, OutOfOrderTest::testColumnTopLastDataMerge2Data0);
    }

    @Test
    public void testColumnTopLastDataMergeDataContended() throws Exception {
        executeWithPool(0, OutOfOrderTest::testColumnTopLastDataMergeData0);
    }

    @Test
    public void testColumnTopLastDataMergeDataParallel() throws Exception {
        executeWithPool(4, OutOfOrderTest::testColumnTopLastDataMergeData0);
    }

    @Test
    public void testColumnTopLastDataOOOData() throws Exception {
        executeVanilla(() -> testColumnTopLastDataOOOData0(
                engine,
                compiler,
                sqlExecutionContext
        ));
    }

    @Test
    public void testColumnTopLastDataOOODataContended() throws Exception {
        executeWithPool(0, OutOfOrderTest::testColumnTopLastDataOOOData0);
    }

    @Test
    public void testColumnTopLastDataOOODataParallel() throws Exception {
        executeWithPool(4, OutOfOrderTest::testColumnTopLastDataOOOData0);
    }

    @Test
    public void testColumnTopLastOOOData() throws Exception {
        executeVanilla(() -> testColumnTopLastOOOData0(
                engine,
                compiler,
                sqlExecutionContext
        ));
    }

    @Test
    public void testColumnTopLastOOODataContended() throws Exception {
        executeWithPool(0, OutOfOrderTest::testColumnTopLastOOOData0);
    }

    @Test
    public void testColumnTopLastOOODataParallel() throws Exception {
        executeWithPool(4, OutOfOrderTest::testColumnTopLastOOOData0);
    }

    @Test
    public void testColumnTopLastOOOPrefix() throws Exception {
        executeVanilla(() -> testColumnTopLastOOOPrefix0(
                engine,
                compiler,
                sqlExecutionContext
        ));
    }

    @Test
    public void testColumnTopLastOOOPrefixContended() throws Exception {
        executeWithPool(0, OutOfOrderTest::testColumnTopLastOOOPrefix0);
    }

    @Test
    public void testColumnTopLastOOOPrefixParallel() throws Exception {
        executeWithPool(4, OutOfOrderTest::testColumnTopLastOOOPrefix0);
    }

    @Test
    public void testColumnTopLastParallel() throws Exception {
        executeWithPool(4, OutOfOrderTest::testColumnTopLastAppendColumn0);
    }

    @Test
    public void testColumnTopMidAppend() throws Exception {
        executeVanilla(() -> testColumnTopMidAppendColumn0(
                engine,
                compiler,
                sqlExecutionContext
        ));
    }

    @Test
    public void testColumnTopMidAppendBlank() throws Exception {
        executeVanilla(() -> testColumnTopMidAppendBlankColumn0(
                engine,
                compiler,
                sqlExecutionContext
        ));
    }

    @Test
    public void testColumnTopMidAppendBlankContended() throws Exception {
        executeWithPool(0, OutOfOrderTest::testColumnTopMidAppendBlankColumn0);
    }

    @Test
    public void testColumnTopMidAppendBlankParallel() throws Exception {
        executeWithPool(4, OutOfOrderTest::testColumnTopMidAppendBlankColumn0);
    }

    @Test
    public void testColumnTopMidAppendContended() throws Exception {
        executeWithPool(0, OutOfOrderTest::testColumnTopMidAppendColumn0);
    }

    @Test
    public void testColumnTopMidAppendParallel() throws Exception {
        executeWithPool(4, OutOfOrderTest::testColumnTopMidAppendColumn0);
    }

    @Test
    public void testColumnTopMidDataMergeData() throws Exception {
        executeVanilla(() -> testColumnTopMidDataMergeData0(
                engine,
                compiler,
                sqlExecutionContext
        ));
    }

    @Test
    public void testColumnTopMidDataMergeDataContended() throws Exception {
        executeWithPool(0, OutOfOrderTest::testColumnTopMidDataMergeData0);
    }

    @Test
    public void testColumnTopMidDataMergeDataParallel() throws Exception {
        executeWithPool(4, OutOfOrderTest::testColumnTopMidDataMergeData0);
    }

    @Test
    public void testColumnTopMidMergeBlank() throws Exception {
        executeVanilla(() -> testColumnTopMidMergeBlankColumn0(
                engine,
                compiler,
                sqlExecutionContext
        ));
    }

    @Test
    public void testColumnTopMidMergeBlankContended() throws Exception {
        executeWithPool(0, OutOfOrderTest::testColumnTopMidMergeBlankColumn0);
    }

    @Test
    public void testColumnTopMidMergeBlankParallel() throws Exception {
        executeWithPool(4, OutOfOrderTest::testColumnTopMidMergeBlankColumn0);
    }

    @Test
    public void testColumnTopMidOOOData() throws Exception {
        executeVanilla(() -> testColumnTopMidOOOData0(
                engine,
                compiler,
                sqlExecutionContext
        ));
    }

    @Test
    public void testColumnTopMidOOODataContended() throws Exception {
        executeWithPool(0, OutOfOrderTest::testColumnTopMidOOOData0);
    }

    @Test
    public void testColumnTopMidOOODataParallel() throws Exception {
        executeWithPool(4, OutOfOrderTest::testColumnTopMidOOOData0);
    }

    @Test
    public void testColumnTopNewPartitionMiddleOfTableContended() throws Exception {
        executeWithPool(0, OutOfOrderTest::testColumnTopNewPartitionMiddleOfTable0);
    }

    @Test
    public void testPartitionedDataAppendOOData() throws Exception {
        executeVanilla(
                () -> testPartitionedDataAppendOOData0(
                        engine,
                        compiler,
                        sqlExecutionContext
                )
        );
    }

    @Test
    public void testPartitionedDataAppendOODataContended() throws Exception {
        executeWithPool(0, OutOfOrderTest::testPartitionedDataAppendOOData0);
    }

    @Test
    public void testPartitionedDataAppendOODataIndexed() throws Exception {
        executeVanilla(() -> testPartitionedDataAppendOODataIndexed0(
                engine,
                compiler,
                sqlExecutionContext
                )
        );
    }

    @Test
    public void testPartitionedDataAppendOODataIndexedContended() throws Exception {
        executeWithPool(0, OutOfOrderTest::testPartitionedDataAppendOODataIndexed0);
    }

    @Test
    public void testPartitionedDataAppendOODataIndexedParallel() throws Exception {
        executeWithPool(4, OutOfOrderTest::testPartitionedDataAppendOODataIndexed0);
    }

    @Test
    public void testPartitionedDataAppendOODataNotNullStrTail() throws Exception {
        executeVanilla(() -> testPartitionedDataAppendOODataNotNullStrTail0(engine, compiler, sqlExecutionContext));
    }

    @Test
    public void testPartitionedDataAppendOODataNotNullStrTailContended() throws Exception {
        executeWithPool(0, OutOfOrderTest::testPartitionedDataAppendOODataNotNullStrTail0);
    }

    @Test
    public void testPartitionedDataAppendOODataNotNullStrTailParallel() throws Exception {
        executeWithPool(4, OutOfOrderTest::testPartitionedDataAppendOODataNotNullStrTail0);
    }

    @Test
    public void testPartitionedDataAppendOODataParallel() throws Exception {
        executeWithPool(4, OutOfOrderTest::testPartitionedDataAppendOOData0);
    }

    @Test
    public void testPartitionedDataAppendOOPrependOOData() throws Exception {
        executeVanilla(() -> testPartitionedDataAppendOOPrependOOData0(
                engine,
                compiler,
                sqlExecutionContext
                )
        );
    }

    @Test
    public void testPartitionedDataAppendOOPrependOODataContended() throws Exception {
        executeWithPool(0, OutOfOrderTest::testPartitionedDataAppendOOPrependOOData0);
    }

    @Test
    public void testPartitionedDataAppendOOPrependOODataParallel() throws Exception {
        executeWithPool(4, OutOfOrderTest::testPartitionedDataAppendOOPrependOOData0);
    }

    @Test
    public void testPartitionedDataMergeData() throws Exception {
        executeVanilla(() -> testPartitionedDataMergeData0(engine, compiler, sqlExecutionContext));
    }

    @Test
    public void testPartitionedDataMergeDataContended() throws Exception {
        executeWithPool(0, OutOfOrderTest::testPartitionedDataMergeData0);
    }

    @Test
    public void testPartitionedDataMergeDataParallel() throws Exception {
        executeWithPool(4, OutOfOrderTest::testPartitionedDataMergeData0);
    }

    @Test
    public void testPartitionedDataMergeEnd() throws Exception {
        executeVanilla(() -> testPartitionedDataMergeEnd0(engine, compiler, sqlExecutionContext));
    }

    @Test
    public void testPartitionedDataMergeEndContended() throws Exception {
        executeWithPool(0, OutOfOrderTest::testPartitionedDataMergeEnd0);
    }

    @Test
    public void testPartitionedDataMergeEndParallel() throws Exception {
        executeWithPool(4, OutOfOrderTest::testPartitionedDataMergeEnd0);
    }

    @Test
    public void testPartitionedDataOOData() throws Exception {
        executeVanilla(() -> testPartitionedDataOOData0(engine, compiler, sqlExecutionContext));
    }

    @Test
    public void testPartitionedDataOODataContended() throws Exception {
        executeWithPool(0, OutOfOrderTest::testPartitionedDataOOData0);
    }

    @Test
    public void testPartitionedDataOODataParallel() throws Exception {
        executeWithPool(4, OutOfOrderTest::testPartitionedDataOOData0);
    }

    @Test
    public void testPartitionedDataOODataPbOOData() throws Exception {
        executeVanilla(() -> testPartitionedDataOODataPbOOData0(
                engine,
                compiler,
                sqlExecutionContext
        ));
    }

    @Test
    public void testPartitionedDataOODataPbOODataContended() throws Exception {
        executeWithPool(0, OutOfOrderTest::testPartitionedDataOODataPbOOData0);
    }

    @Test
    public void testPartitionedDataOODataPbOODataParallel() throws Exception {
        executeWithPool(4, OutOfOrderTest::testPartitionedDataOODataPbOOData0);
    }

    @Test
    public void testPartitionedDataOOIntoLastIndexSearchBug() throws Exception {
        executeVanilla(() -> testPartitionedDataOOIntoLastIndexSearchBug0(engine, compiler, sqlExecutionContext));
    }

    @Test
    public void testPartitionedDataOOIntoLastIndexSearchBugContended() throws Exception {
        executeWithPool(0, OutOfOrderTest::testPartitionedDataOOIntoLastIndexSearchBug0);
    }

    @Test
    public void testPartitionedDataOOIntoLastIndexSearchBugParallel() throws Exception {
        executeWithPool(4, OutOfOrderTest::testPartitionedDataOOIntoLastIndexSearchBug0);
    }

    @Test
    public void testPartitionedDataOOIntoLastOverflowIntoNewPartition() throws Exception {
        executeVanilla(() -> testPartitionedDataOOIntoLastOverflowIntoNewPartition0(engine, compiler, sqlExecutionContext));
    }

    @Test
    public void testPartitionedDataOOIntoLastOverflowIntoNewPartitionContended() throws Exception {
        executeWithPool(0, OutOfOrderTest::testPartitionedDataOOIntoLastOverflowIntoNewPartition0);
    }

    @Test
    public void testPartitionedDataOOIntoLastOverflowIntoNewPartitionParallel() throws Exception {
        executeWithPool(4, OutOfOrderTest::testPartitionedDataOOIntoLastOverflowIntoNewPartition0);
    }

    @Test
    public void testPartitionedOOData() throws Exception {
        executeVanilla(() -> testPartitionedOOData0(engine, compiler, sqlExecutionContext));
    }

    @Test
    public void testPartitionedOODataContended() throws Exception {
        executeWithPool(0, OutOfOrderTest::testPartitionedOOData0);
    }

    @Test
    public void testPartitionedOODataOOCollapsed() throws Exception {
        executeVanilla(() -> testPartitionedOODataOOCollapsed0(engine, compiler, sqlExecutionContext));
    }

    @Test
    public void testPartitionedOODataOOCollapsedContended() throws Exception {
        executeWithPool(0, OutOfOrderTest::testPartitionedOODataOOCollapsed0);
    }

    @Test
    public void testPartitionedOODataOOCollapsedParallel() throws Exception {
        executeWithPool(4, OutOfOrderTest::testPartitionedOODataOOCollapsed0);
    }

    @Test
    public void testPartitionedOODataParallel() throws Exception {
        executeWithPool(4, OutOfOrderTest::testPartitionedOOData0);
    }

    @Test
    public void testPartitionedOODataUpdateMinTimestamp() throws Exception {
        executeVanilla(() -> testPartitionedOODataUpdateMinTimestamp0(engine, compiler, sqlExecutionContext));
    }

    @Test
    public void testPartitionedOODataUpdateMinTimestampContended() throws Exception {
        executeWithPool(0, OutOfOrderTest::testPartitionedOODataUpdateMinTimestamp0);
    }

    @Test
    public void testPartitionedOODataUpdateMinTimestampParallel() throws Exception {
        executeWithPool(4, OutOfOrderTest::testPartitionedOODataUpdateMinTimestamp0);
    }

    @Test
    public void testPartitionedOOMerge() throws Exception {
        executeVanilla(() -> testPartitionedOOMerge0(engine, compiler, sqlExecutionContext));
    }

    @Test
    public void testPartitionedOOMergeContended() throws Exception {
        executeWithPool(0, OutOfOrderTest::testPartitionedOOMerge0);
    }

    @Test
    public void testPartitionedOOMergeData() throws Exception {
        executeVanilla(() -> testPartitionedOOMergeData0(engine, compiler, sqlExecutionContext));
    }

    @Test
    public void testPartitionedOOMergeDataContended() throws Exception {
        executeWithPool(0, OutOfOrderTest::testPartitionedOOMergeData0);
    }

    @Test
    public void testPartitionedOOMergeDataParallel() throws Exception {
        executeWithPool(4, OutOfOrderTest::testPartitionedOOMergeData0);
    }

    @Test
    public void testPartitionedOOMergeOO() throws Exception {
        executeVanilla(() -> testPartitionedOOMergeOO0(engine, compiler, sqlExecutionContext));
    }

    @Test
    public void testPartitionedOOMergeOOContended() throws Exception {
        executeWithPool(0, OutOfOrderTest::testPartitionedOOMergeOO0);
    }

    @Test
    public void testPartitionedOOMergeOOParallel() throws Exception {
        executeWithPool(4, OutOfOrderTest::testPartitionedOOMergeOO0);
    }

    @Test
    public void testPartitionedOOMergeParallel() throws Exception {
        executeWithPool(4, OutOfOrderTest::testPartitionedOOMerge0);
    }

    @Test
    public void testPartitionedOOONullSetters() throws Exception {
        executeVanilla(() -> {

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

            printSqlResult(compiler, sqlExecutionContext, sink, "x");

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
        executeVanilla(() -> testPartitionedOOPrefixesExistingPartitions0(engine, compiler, sqlExecutionContext));
    }

    @Test
    public void testPartitionedOOPrefixesExistingPartitionsContended() throws Exception {
        executeWithPool(0, OutOfOrderTest::testPartitionedOOPrefixesExistingPartitions0);
    }

    @Test
    public void testPartitionedOOPrefixesExistingPartitionsParallel() throws Exception {
        executeWithPool(4, OutOfOrderTest::testPartitionedOOPrefixesExistingPartitions0);
    }

    @Test
    public void testPartitionedOOTopAndBottom() throws Exception {
        executeVanilla(() -> testPartitionedOOTopAndBottom0(engine, compiler, sqlExecutionContext));
    }

    @Test
    public void testPartitionedOOTopAndBottomContended() throws Exception {
        executeWithPool(0, OutOfOrderTest::testPartitionedOOTopAndBottom0);
    }

    @Test
    public void testPartitionedOOTopAndBottomParallel() throws Exception {
        executeWithPool(4, OutOfOrderTest::testPartitionedOOTopAndBottom0);
    }

    private static void testPartitionedDataAppendOOPrependOOData0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
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
                engine,
                compiler,
                sqlExecutionContext,
                "create table y as (x union all append)",
                "y order by ts",
                "insert into x select * from append",
                "x"
        );

        assertIndexConsistency(
                compiler,
                sqlExecutionContext
        );
    }

    private static void executeVanilla(TestUtils.LeakProneCode code) throws Exception {
        OutOfOrderUtils.initBuf();
        try {
            assertMemoryLeak(code);
        } finally {
            OutOfOrderUtils.freeBuf();
        }
    }

    private static void testPartitionedOOPrefixesExistingPartitions0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
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
                engine,
                compiler,
                sqlExecutionContext,
                "create table y as (select * from x union all select * from top)",
                "y order by ts",
                "insert into x select * from top",
                "x"
        );

        assertIndexConsistency(compiler, sqlExecutionContext);
    }

    private static void testPartitionedOOTopAndBottom0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
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
                engine,
                compiler,
                sqlExecutionContext,
                "create table y as (select * from x union all select * from top union all select * from bottom)",
                "y order by ts",
                "insert into x select * from (top union all bottom)",
                "x"
        );

        assertIndexConsistency(compiler, sqlExecutionContext);
    }

    private static void testPartitionedOOMergeOO0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
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

        printSqlResult(compiler, sqlExecutionContext, sink, sqlTemplate + "y");

        String expected = Chars.toString(sink);

        printSqlResult(compiler, sqlExecutionContext, sink, sqlTemplate + "x");
        TestUtils.assertEquals(expected, sink);

        assertIndexConsistency(compiler, sqlExecutionContext);
    }

    private static void testPartitionedOOMergeData0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
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
        printSqlResult(compiler, sqlExecutionContext, sink, "y order by ts");

        String expected = Chars.toString(sink);

        engine.releaseAllReaders();

        // insert 1AM data into X
        compiler.compile("insert into x select * from 1am", sqlExecutionContext);
        compiler.compile("insert into x select * from tail", sqlExecutionContext);

        printSqlResult(compiler, sqlExecutionContext, sink, "x");

        TestUtils.assertEquals(expected, sink);

        assertIndexConsistency(compiler, sqlExecutionContext);
    }

    private static void testPartitionedOODataUpdateMinTimestamp0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
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
                engine,
                compiler,
                sqlExecutionContext,
                "create table y as (select * from x union all select * from 1am union all select * from prev)",
                "y order by ts",
                "insert into x select * from (1am union all prev)",
                "x"
        );

        assertIndexConsistency(compiler, sqlExecutionContext);
    }

    private static void testPartitionedOOMerge0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
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
        printSqlResult(compiler, sqlExecutionContext, sink, "y order by ts");

        String expected = Chars.toString(sink);

        // close reader to enable "rename" to succeed
        engine.releaseAllReaders();

        // insert 1AM data into X
        compiler.compile("insert into x select * from 1am", sqlExecutionContext);
        compiler.compile("insert into x select * from tail", sqlExecutionContext);

        printSqlResult(compiler, sqlExecutionContext, sink, "x");

        TestUtils.assertEquals(expected, sink);

        //
        engine.releaseAllReaders();

        printSqlResult(compiler, sqlExecutionContext, sink, "x");

        TestUtils.assertEquals(expected, sink);

        assertIndexConsistency(compiler, sqlExecutionContext);
    }

    private static void testPartitionedOODataOOCollapsed0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
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
                engine,
                compiler,
                sqlExecutionContext,
                "create table y as (x union all middle)",
                "y order by ts",
                "insert into x select * from middle",
                "x"
        );

        assertIndexConsistency(compiler, sqlExecutionContext);
    }

    private static void testPartitionedOOData0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
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
        printSqlResult(compiler, sqlExecutionContext, sink, "y order by ts");

        String expected = Chars.toString(sink);

        engine.releaseAllReaders();

        // insert 1AM data into X
        compiler.compile("insert into x select * from 1am", sqlExecutionContext);
        compiler.compile("insert into x select * from tail", sqlExecutionContext);

        printSqlResult(compiler, sqlExecutionContext, sink, "x");

        TestUtils.assertEquals(expected, sink);

        engine.releaseAllReaders();

        printSqlResult(compiler, sqlExecutionContext, sink, "x");

        TestUtils.assertEquals(expected, sink);

        assertIndexConsistency(compiler, sqlExecutionContext);
    }

    private static void testPartitionedDataOOIntoLastOverflowIntoNewPartition0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException, URISyntaxException {
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
        printSqlResult(compiler, sqlExecutionContext, sink, "y order by ts");

//                    String expected = Chars.toString(sink);
        engine.releaseAllReaders();

        compiler.compile("insert into x select * from 1am", sqlExecutionContext);
        compiler.compile("insert into x select * from tail", sqlExecutionContext);

        // It is necessary to release cached "x" reader because as of yet
        // reader cannot reload any partition other than "current".
        // Cached reader will assume smaller partition size for "1970-01-01", which out-of-order insert updated
        engine.releaseAllReaders();

        assertSqlResultAgainstFile(
                compiler,
                sqlExecutionContext,
                "x",
                "/oo/testPartitionedDataOOIntoLastOverflowIntoNewPartition.txt"
        );

        assertIndexConsistency(compiler, sqlExecutionContext);
    }

    private static void testPartitionedDataOOIntoLastIndexSearchBug0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
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
        printSqlResult(compiler, sqlExecutionContext, sink, "y order by ts");

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

        printSqlResult(compiler, sqlExecutionContext, sink, "x");

        TestUtils.assertEquals(expected, sink);

        assertIndexConsistency(compiler, sqlExecutionContext);
    }

    private static void testPartitionedDataMergeData0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException, URISyntaxException {
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
                engine,
                compiler,
                sqlExecutionContext,
                "create table y as (x union all middle)",
                "insert into x select * from middle",
                "/oo/testPartitionedDataMergeData.txt"
        );

        assertIndexResultAgainstFile(
                compiler,
                sqlExecutionContext,
                "/oo/testPartitionedDataMergeData_Index.txt"
        );
    }

    private static void testPartitionedDataMergeEnd0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException, URISyntaxException {
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
                engine,
                compiler,
                sqlExecutionContext,
                "create table y as (x union all middle)",
                "insert into x select * from middle",
                "/oo/testPartitionedDataMergeEnd.txt"
        );

        assertIndexResultAgainstFile(
                compiler,
                sqlExecutionContext,
                "/oo/testPartitionedDataMergeEnd_Index.txt"
        );
    }

    private static void testPartitionedDataOOData0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException, URISyntaxException {
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
                engine,
                compiler,
                sqlExecutionContext,
                "create table y as (x union all middle)",
                "insert into x select * from middle",
                "/oo/testPartitionedDataOOData.txt"
        );

        assertIndexConsistency(compiler, sqlExecutionContext);
    }

    private static void testPartitionedDataAppendOODataNotNullStrTail0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
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
                engine,
                compiler,
                sqlExecutionContext,
                "create table y as (x union all append)",
                "y order by ts",
                "insert into x select * from append",
                "x"
        );

        assertIndexConsistency(compiler, sqlExecutionContext);
    }

    private static void assertOutOfOrderDataConsistency(
            final CairoEngine engine,
            final SqlCompiler compiler,
            final SqlExecutionContext sqlExecutionContext,
            final String referenceTableDDL,
            final String referenceSQL,
            final String outOfOrderInsertSQL,
            final String assertSQL
    ) throws SqlException {
        sink.clear();
        sink2.clear();
        assertOutOfOrderDataConsistency(
                engine,
                compiler,
                sqlExecutionContext,
                referenceTableDDL,
                referenceSQL,
                outOfOrderInsertSQL,
                assertSQL,
                sink,
                sink2
        );
    }

    private static void assertOutOfOrderDataConsistency(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            String referenceTableDDL,
            String referenceSQL,
            String outOfOrderInsertSQL,
            String assertSQL,
            CharSink sink1,
            CharSink sink2
    ) throws SqlException {
        // create third table, which will contain both X and 1AM
        compiler.compile(referenceTableDDL, sqlExecutionContext);

            // expected outcome
            printSqlResult0(compiler, sqlExecutionContext, sink1, referenceSQL);

            // release reader "before" out-of-order is handled
            // we aim directory rename operations to succeed on Windows
            // Linux should be fine without closing readers
            engine.releaseAllReaders();

            compiler.compile(outOfOrderInsertSQL, sqlExecutionContext);

            // todo: ensure reader can pick up out of order stuff
            // release reader for now because it is unable to reload out-of-order results
            engine.releaseAllReaders();

            printSqlResult0(compiler, sqlExecutionContext, sink2, assertSQL);

            TestUtils.assertEquals((CharSequence) sink1, (CharSequence) sink2);
    }

    private static void testPartitionedDataAppendOODataIndexed0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
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
                engine,
                compiler,
                sqlExecutionContext,
                "create table y as (x union all append)",
                "y where sym = 'googl' order by ts",
                "insert into x select * from append",
                "x where sym = 'googl'"
        );
    }

    private static void testColumnTopLastDataOOOData0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException, URISyntaxException {

        //
        // ----- last partition
        //
        // +-----------+
        // |   empty   |
        // |           |
        // +-----------+   <-- top -->       +---------+
        // |           |                     |   data  |
        // |           |   +----------+      +---------+
        // |           |   |   OOO    |      |   ooo   |
        // |           | < |  block   |  ==  +---------+
        // |           |   | (narrow) |      |   data  |
        // |           |   +----------+      +---------+
        // +-----------+
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

        compiler.compile("alter table x add column v double", sqlExecutionContext);
        compiler.compile("alter table x add column v1 float", sqlExecutionContext);
        compiler.compile("alter table x add column v2 int", sqlExecutionContext);
        compiler.compile("alter table x add column v3 byte", sqlExecutionContext);
        compiler.compile("alter table x add column v4 short", sqlExecutionContext);
        compiler.compile("alter table x add column v5 boolean", sqlExecutionContext);
        compiler.compile("alter table x add column v6 date", sqlExecutionContext);
        compiler.compile("alter table x add column v7 timestamp", sqlExecutionContext);
        compiler.compile("alter table x add column v8 symbol", sqlExecutionContext);
        compiler.compile("alter table x add column v10 char", sqlExecutionContext);
        compiler.compile("alter table x add column v11 string", sqlExecutionContext);
        compiler.compile("alter table x add column v12 binary", sqlExecutionContext);
        compiler.compile("alter table x add column v9 long", sqlExecutionContext);

        compiler.compile(
                "insert into x " +
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
                        " timestamp_sequence(549920000000L,100000000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
//        --------     new columns here ---------------
                        " rnd_double() v," +
                        " rnd_float() v1," +
                        " rnd_int() v2," +
                        " rnd_byte() v3," +
                        " rnd_short() v4," +
                        " rnd_boolean() v5," +
                        " rnd_date() v6," +
                        " rnd_timestamp(10,100000,356) v7," +
                        " rnd_symbol('AAA','BBB', null) v8," +
                        " rnd_char() v10," +
                        " rnd_str() v11," +
                        " rnd_bin() v12," +
                        " rnd_long() v9" +
                        " from long_sequence(500)",
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
                        " timestamp_sequence(549920000000L,100000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
//        --------     new columns here ---------------
                        " rnd_double() v," +
                        " rnd_float() v1," +
                        " rnd_int() v2," +
                        " rnd_byte() v3," +
                        " rnd_short() v4," +
                        " rnd_boolean() v5," +
                        " rnd_date() v6," +
                        " rnd_timestamp(10,100000,356) v7," +
                        " rnd_symbol('AAA','BBB', null) v8," +
                        " rnd_char() v10," +
                        " rnd_str() v11," +
                        " rnd_bin() v12," +
                        " rnd_long() v9" +
                        " from long_sequence(100)" +
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        compiler.compile("insert into x select * from append", sqlExecutionContext);

        // use this code to debug
//        assertOutOfOrderDataConsistency(
//                engine,
//                compiler,
//                sqlExecutionContext,
//                "create table y as (x union all append)",
//                "y order by ts",
//                "insert into x select * from append",
//                "x'"
//        );

        assertSqlResultAgainstFile(
                compiler,
                sqlExecutionContext,
                "x",
                "/oo/testColumnTopLastDataOOOData.txt"
        );
    }

    private static void testColumnTopLastDataMergeData0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException, URISyntaxException {

        //
        // ----- last partition
        //
        // +-----------+
        // |   empty   |
        // |           |
        // +-----------+   <-- top -->       +---------+
        // |           |                     |   data  |
        // |           |   +----------+      +---------+
        // |           |   |   OOO    |      |   ooo   |
        // |           | < |  block   |  ==  +---------+
        // |           |   | (narrow) |      |   data  |
        // |           |   +----------+      +---------+
        // +-----------+
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

        compiler.compile("alter table x add column v double", sqlExecutionContext);
        compiler.compile("alter table x add column v1 float", sqlExecutionContext);
        compiler.compile("alter table x add column v2 int", sqlExecutionContext);
        compiler.compile("alter table x add column v3 byte", sqlExecutionContext);
        compiler.compile("alter table x add column v4 short", sqlExecutionContext);
        compiler.compile("alter table x add column v5 boolean", sqlExecutionContext);
        compiler.compile("alter table x add column v6 date", sqlExecutionContext);
        compiler.compile("alter table x add column v7 timestamp", sqlExecutionContext);
        compiler.compile("alter table x add column v8 symbol", sqlExecutionContext);
        compiler.compile("alter table x add column v10 char", sqlExecutionContext);
        compiler.compile("alter table x add column v11 string", sqlExecutionContext);
        compiler.compile("alter table x add column v12 binary", sqlExecutionContext);
        compiler.compile("alter table x add column v9 long", sqlExecutionContext);

        compiler.compile(
                "insert into x " +
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
                        " timestamp_sequence(549920000000L,100000000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
//        --------     new columns here ---------------
                        " rnd_double() v," +
                        " rnd_float() v1," +
                        " rnd_int() v2," +
                        " rnd_byte() v3," +
                        " rnd_short() v4," +
                        " rnd_boolean() v5," +
                        " rnd_date() v6," +
                        " rnd_timestamp(10,100000,356) v7," +
                        " rnd_symbol('AAA','BBB', null) v8," +
                        " rnd_char() v10," +
                        " rnd_str() v11," +
                        " rnd_bin() v12," +
                        " rnd_long() v9" +
                        " from long_sequence(500)",
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
                        " timestamp_sequence(549900000000L,50000000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
//        --------     new columns here ---------------
                        " rnd_double() v," +
                        " rnd_float() v1," +
                        " rnd_int() v2," +
                        " rnd_byte() v3," +
                        " rnd_short() v4," +
                        " rnd_boolean() v5," +
                        " rnd_date() v6," +
                        " rnd_timestamp(10,100000,356) v7," +
                        " rnd_symbol('AAA','BBB', null) v8," +
                        " rnd_char() v10," +
                        " rnd_str() v11," +
                        " rnd_bin() v12," +
                        " rnd_long() v9" +
                        " from long_sequence(100)" +
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        compiler.compile("insert into x select * from append", sqlExecutionContext);

        // use this code to debug
//        assertOutOfOrderDataConsistency(
//                engine,
//                compiler,
//                sqlExecutionContext,
//                "create table y as (x union all append)",
//                "y order by ts",
//                "insert into x select * from append",
//                "x'"
//        );
//
        assertSqlResultAgainstFile(
                compiler,
                sqlExecutionContext,
                "x",
                "/oo/testColumnTopLastDataMergeData.txt"
        );
    }


    private static void testColumnTopMidDataMergeData0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException, URISyntaxException {

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

        compiler.compile("alter table x add column v double", sqlExecutionContext);
        compiler.compile("alter table x add column v1 float", sqlExecutionContext);
        compiler.compile("alter table x add column v2 int", sqlExecutionContext);
        compiler.compile("alter table x add column v3 byte", sqlExecutionContext);
        compiler.compile("alter table x add column v4 short", sqlExecutionContext);
        compiler.compile("alter table x add column v5 boolean", sqlExecutionContext);
        compiler.compile("alter table x add column v6 date", sqlExecutionContext);
        compiler.compile("alter table x add column v7 timestamp", sqlExecutionContext);
        compiler.compile("alter table x add column v8 symbol", sqlExecutionContext);
        compiler.compile("alter table x add column v10 char", sqlExecutionContext);
        compiler.compile("alter table x add column v11 string", sqlExecutionContext);
        compiler.compile("alter table x add column v12 binary", sqlExecutionContext);
        compiler.compile("alter table x add column v9 long", sqlExecutionContext);

        compiler.compile(
                "insert into x " +
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
                        " timestamp_sequence(549920000000L,100000000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
//        --------     new columns here ---------------
                        " rnd_double() v," +
                        " rnd_float() v1," +
                        " rnd_int() v2," +
                        " rnd_byte() v3," +
                        " rnd_short() v4," +
                        " rnd_boolean() v5," +
                        " rnd_date() v6," +
                        " rnd_timestamp(10,100000,356) v7," +
                        " rnd_symbol('AAA','BBB', null) v8," +
                        " rnd_char() v10," +
                        " rnd_str() v11," +
                        " rnd_bin() v12," +
                        " rnd_long() v9" +
                        " from long_sequence(1000)",
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
                        " timestamp_sequence(549900000000L,50000000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
//        --------     new columns here ---------------
                        " rnd_double() v," +
                        " rnd_float() v1," +
                        " rnd_int() v2," +
                        " rnd_byte() v3," +
                        " rnd_short() v4," +
                        " rnd_boolean() v5," +
                        " rnd_date() v6," +
                        " rnd_timestamp(10,100000,356) v7," +
                        " rnd_symbol('AAA','BBB', null) v8," +
                        " rnd_char() v10," +
                        " rnd_str() v11," +
                        " rnd_bin() v12," +
                        " rnd_long() v9" +
                        " from long_sequence(100)" +
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        compiler.compile("insert into x select * from append", sqlExecutionContext);

        // use this code to debug
//        assertOutOfOrderDataConsistency(
//                engine,
//                compiler,
//                sqlExecutionContext,
//                "create table y as (x union all append)",
//                "y order by ts",
//                "insert into x select * from append",
//                "x'"
//        );

        assertSqlResultAgainstFile(
                compiler,
                sqlExecutionContext,
                "x",
                "/oo/testColumnTopMidDataMergeData.txt"
        );
    }

    private static void testColumnTopLastDataMerge2Data0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException, URISyntaxException {

        // merge in the middle, however data prefix is such
        // that we can deal with by reducing column top rather than copying columns

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

        compiler.compile("alter table x add column v double", sqlExecutionContext);
        compiler.compile("alter table x add column v1 float", sqlExecutionContext);
        compiler.compile("alter table x add column v2 int", sqlExecutionContext);
        compiler.compile("alter table x add column v3 byte", sqlExecutionContext);
        compiler.compile("alter table x add column v4 short", sqlExecutionContext);
        compiler.compile("alter table x add column v5 boolean", sqlExecutionContext);
        compiler.compile("alter table x add column v6 date", sqlExecutionContext);
        compiler.compile("alter table x add column v7 timestamp", sqlExecutionContext);
        compiler.compile("alter table x add column v8 symbol", sqlExecutionContext);
        compiler.compile("alter table x add column v10 char", sqlExecutionContext);
        compiler.compile("alter table x add column v11 string", sqlExecutionContext);
        compiler.compile("alter table x add column v12 binary", sqlExecutionContext);
        compiler.compile("alter table x add column v9 long", sqlExecutionContext);

        compiler.compile(
                "insert into x " +
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
                        " timestamp_sequence(549920000000L,100000000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
//        --------     new columns here ---------------
                        " rnd_double() v," +
                        " rnd_float() v1," +
                        " rnd_int() v2," +
                        " rnd_byte() v3," +
                        " rnd_short() v4," +
                        " rnd_boolean() v5," +
                        " rnd_date() v6," +
                        " rnd_timestamp(10,100000,356) v7," +
                        " rnd_symbol('AAA','BBB', null) v8," +
                        " rnd_char() v10," +
                        " rnd_str() v11," +
                        " rnd_bin() v12," +
                        " rnd_long() v9" +
                        " from long_sequence(500)",
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
                        " timestamp_sequence(549920000000L,50000000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
//        --------     new columns here ---------------
                        " rnd_double() v," +
                        " rnd_float() v1," +
                        " rnd_int() v2," +
                        " rnd_byte() v3," +
                        " rnd_short() v4," +
                        " rnd_boolean() v5," +
                        " rnd_date() v6," +
                        " rnd_timestamp(10,100000,356) v7," +
                        " rnd_symbol('AAA','BBB', null) v8," +
                        " rnd_char() v10," +
                        " rnd_str() v11," +
                        " rnd_bin() v12," +
                        " rnd_long() v9" +
                        " from long_sequence(100)" +
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        compiler.compile("insert into x select * from append", sqlExecutionContext);

        // use this code to debug
//        assertOutOfOrderDataConsistency(
//                engine,
//                compiler,
//                sqlExecutionContext,
//                "create table y as (x union all append)",
//                "y order by ts",
//                "insert into x select * from append",
//                "x'"
//        );

        assertSqlResultAgainstFile(
                compiler,
                sqlExecutionContext,
                "x",
                "/oo/testColumnTopLastDataMerge2Data.txt"
        );
    }

    private static void testColumnTopLastOOOPrefix0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException, URISyntaxException {
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
                        " timestamp_sequence(500000000000L,330000000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t" +
                        " from long_sequence(500)" +
                        "), index(sym) timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        compiler.compile("alter table x add column v double", sqlExecutionContext);
        compiler.compile("alter table x add column v1 float", sqlExecutionContext);
        compiler.compile("alter table x add column v2 int", sqlExecutionContext);
        compiler.compile("alter table x add column v3 byte", sqlExecutionContext);
        compiler.compile("alter table x add column v4 short", sqlExecutionContext);
        compiler.compile("alter table x add column v5 boolean", sqlExecutionContext);
        compiler.compile("alter table x add column v6 date", sqlExecutionContext);
        compiler.compile("alter table x add column v7 timestamp", sqlExecutionContext);
        compiler.compile("alter table x add column v8 symbol", sqlExecutionContext);
        compiler.compile("alter table x add column v10 char", sqlExecutionContext);
        compiler.compile("alter table x add column v11 string", sqlExecutionContext);
        compiler.compile("alter table x add column v12 binary", sqlExecutionContext);
        compiler.compile("alter table x add column v9 long", sqlExecutionContext);

        compiler.compile(
                "insert into x " +
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
                        " timestamp_sequence(664670000000L,10000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
//        --------     new columns here ---------------
                        " rnd_double() v," +
                        " rnd_float() v1," +
                        " rnd_int() v2," +
                        " rnd_byte() v3," +
                        " rnd_short() v4," +
                        " rnd_boolean() v5," +
                        " rnd_date() v6," +
                        " rnd_timestamp(10,100000,356) v7," +
                        " rnd_symbol('AAA','BBB', null) v8," +
                        " rnd_char() v10," +
                        " rnd_str() v11," +
                        " rnd_bin() v12," +
                        " rnd_long() v9" +
                        " from long_sequence(500)",
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
                        " timestamp_sequence(604800000000L,10000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
//        --------     new columns here ---------------
                        " rnd_double() v," +
                        " rnd_float() v1," +
                        " rnd_int() v2," +
                        " rnd_byte() v3," +
                        " rnd_short() v4," +
                        " rnd_boolean() v5," +
                        " rnd_date() v6," +
                        " rnd_timestamp(10,100000,356) v7," +
                        " rnd_symbol('AAA','BBB', null) v8," +
                        " rnd_char() v10," +
                        " rnd_str() v11," +
                        " rnd_bin() v12," +
                        " rnd_long() v9" +
                        " from long_sequence(400)" +
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        compiler.compile("insert into x select * from append", sqlExecutionContext);

        // use this code to debug
//        assertOutOfOrderDataConsistency(
//                engine,
//                compiler,
//                sqlExecutionContext,
//                "create table y as (x union all append)",
//                "y order by ts",
//                "insert into x select * from append",
//                "x'"
//        );

        assertSqlResultAgainstFile(
                compiler,
                sqlExecutionContext,
                "x",
                "/oo/testColumnTopLastOOOPrefix.txt"
        );
    }

    private static void testColumnTopLastOOOData0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException, URISyntaxException {
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

        compiler.compile("alter table x add column v double", sqlExecutionContext);
        compiler.compile("alter table x add column v1 float", sqlExecutionContext);
        compiler.compile("alter table x add column v2 int", sqlExecutionContext);
        compiler.compile("alter table x add column v3 byte", sqlExecutionContext);
        compiler.compile("alter table x add column v4 short", sqlExecutionContext);
        compiler.compile("alter table x add column v5 boolean", sqlExecutionContext);
        compiler.compile("alter table x add column v6 date", sqlExecutionContext);
        compiler.compile("alter table x add column v7 timestamp", sqlExecutionContext);
        compiler.compile("alter table x add column v8 symbol", sqlExecutionContext);
        compiler.compile("alter table x add column v10 char", sqlExecutionContext);
        compiler.compile("alter table x add column v11 string", sqlExecutionContext);
        compiler.compile("alter table x add column v12 binary", sqlExecutionContext);
        compiler.compile("alter table x add column v9 long", sqlExecutionContext);

        compiler.compile(
                "insert into x " +
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
                        " timestamp_sequence(549920000000L,100000000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
//        --------     new columns here ---------------
                        " rnd_double() v," +
                        " rnd_float() v1," +
                        " rnd_int() v2," +
                        " rnd_byte() v3," +
                        " rnd_short() v4," +
                        " rnd_boolean() v5," +
                        " rnd_date() v6," +
                        " rnd_timestamp(10,100000,356) v7," +
                        " rnd_symbol('AAA','BBB', null) v8," +
                        " rnd_char() v10," +
                        " rnd_str() v11," +
                        " rnd_bin() v12," +
                        " rnd_long() v9" +
                        " from long_sequence(500)",
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
                        " timestamp_sequence(546600000000L,100000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
//        --------     new columns here ---------------
                        " rnd_double() v," +
                        " rnd_float() v1," +
                        " rnd_int() v2," +
                        " rnd_byte() v3," +
                        " rnd_short() v4," +
                        " rnd_boolean() v5," +
                        " rnd_date() v6," +
                        " rnd_timestamp(10,100000,356) v7," +
                        " rnd_symbol('AAA','BBB', null) v8," +
                        " rnd_char() v10," +
                        " rnd_str() v11," +
                        " rnd_bin() v12," +
                        " rnd_long() v9" +
                        " from long_sequence(100)" +
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        compiler.compile("insert into x select * from append", sqlExecutionContext);

        // use this code to debug
//        assertOutOfOrderDataConsistency(
//                engine,
//                compiler,
//                sqlExecutionContext,
//                "create table y as (x union all append)",
//                "y order by ts",
//                "insert into x select * from append",
//                "x'"
//        );

        assertSqlResultAgainstFile(
                compiler,
                sqlExecutionContext,
                "x",
                "/oo/testColumnTopLastOOOData.txt"
        );
    }

    private static void testColumnTopMidOOOData0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException, URISyntaxException {
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

        compiler.compile("alter table x add column v double", sqlExecutionContext);
        compiler.compile("alter table x add column v1 float", sqlExecutionContext);
        compiler.compile("alter table x add column v2 int", sqlExecutionContext);
        compiler.compile("alter table x add column v3 byte", sqlExecutionContext);
        compiler.compile("alter table x add column v4 short", sqlExecutionContext);
        compiler.compile("alter table x add column v5 boolean", sqlExecutionContext);
        compiler.compile("alter table x add column v6 date", sqlExecutionContext);
        compiler.compile("alter table x add column v7 timestamp", sqlExecutionContext);
        compiler.compile("alter table x add column v8 symbol", sqlExecutionContext);
        compiler.compile("alter table x add column v10 char", sqlExecutionContext);
        compiler.compile("alter table x add column v11 string", sqlExecutionContext);
        compiler.compile("alter table x add column v12 binary", sqlExecutionContext);
        compiler.compile("alter table x add column v9 long", sqlExecutionContext);

        compiler.compile(
                "insert into x " +
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
                        " timestamp_sequence(549920000000L,100000000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
//        --------     new columns here ---------------
                        " rnd_double() v," +
                        " rnd_float() v1," +
                        " rnd_int() v2," +
                        " rnd_byte() v3," +
                        " rnd_short() v4," +
                        " rnd_boolean() v5," +
                        " rnd_date() v6," +
                        " rnd_timestamp(10,100000,356) v7," +
                        " rnd_symbol('AAA','BBB', null) v8," +
                        " rnd_char() v10," +
                        " rnd_str() v11," +
                        " rnd_bin() v12," +
                        " rnd_long() v9" +
                        " from long_sequence(1000)",
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
                        " timestamp_sequence(546600000000L,100000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
//        --------     new columns here ---------------
                        " rnd_double() v," +
                        " rnd_float() v1," +
                        " rnd_int() v2," +
                        " rnd_byte() v3," +
                        " rnd_short() v4," +
                        " rnd_boolean() v5," +
                        " rnd_date() v6," +
                        " rnd_timestamp(10,100000,356) v7," +
                        " rnd_symbol('AAA','BBB', null) v8," +
                        " rnd_char() v10," +
                        " rnd_str() v11," +
                        " rnd_bin() v12," +
                        " rnd_long() v9" +
                        " from long_sequence(100)" +
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        compiler.compile("insert into x select * from append", sqlExecutionContext);

        // use this code to debug
//        assertOutOfOrderDataConsistency(
//                engine,
//                compiler,
//                sqlExecutionContext,
//                "create table y as (x union all append)",
//                "y order by ts",
//                "insert into x select * from append",
//                "x'"
//        );

        assertSqlResultAgainstFile(
                compiler,
                sqlExecutionContext,
                "x",
                "/oo/testColumnTopMidOOOData.txt"
        );
    }

    private static void assertIndexConsistency(
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        // index test
        // expected outcome
        printSqlResult(compiler, sqlExecutionContext, sink, "y where sym = 'googl' order by ts");

        String expected = Chars.toString(sink);

        printSqlResult(compiler, sqlExecutionContext, sink, "x where sym = 'googl'");

        TestUtils.assertEquals(expected, sink);
    }

    private static void printSqlResult(SqlCompiler compiler, SqlExecutionContext sqlExecutionContext, StringSink sink, String sql) throws SqlException {
        sink.clear();
        printSqlResult0(compiler, sqlExecutionContext, sink, sql);
    }

    private static void printSqlResult0(SqlCompiler compiler, SqlExecutionContext sqlExecutionContext, CharSink sink, String sql) throws SqlException {
        try (RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                printer.print(cursor, factory.getMetadata(), true, sink);
            }
        }
    }

    private static void assertIndexResultAgainstFile(
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            String resourceName
    ) throws SqlException, URISyntaxException {
        assertSqlResultAgainstFile(compiler, sqlExecutionContext, "x where sym = 'googl'", resourceName);
    }

    private static void assertOutOfOrderDataConsistency(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            final String referenceTableDDL,
            final String outOfOrderSQL,
            final String resourceName
    ) throws SqlException, URISyntaxException {
        // create third table, which will contain both X and 1AM
        compiler.compile(referenceTableDDL, sqlExecutionContext);

        // expected outcome - output ignored, but useful for debug
        printSqlResult(compiler, sqlExecutionContext, sink, "y order by ts");

        engine.releaseAllReaders();

        compiler.compile(outOfOrderSQL, sqlExecutionContext);

        // release reader
        engine.releaseAllReaders();

        assertSqlResultAgainstFile(compiler, sqlExecutionContext, "x", resourceName);
    }

    private static void assertSqlResultAgainstFile(
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            String sql,
            String resourceName
    ) throws URISyntaxException, SqlException {
        printSqlResult(compiler, sqlExecutionContext, sink, sql);

        URL url = OutOfOrderTest.class.getResource(resourceName);
        Assert.assertNotNull(url);
//        System.out.println(sink);
        TestUtils.assertEquals(new File(url.toURI()), sink);
    }

    private static void testPartitionedDataAppendOOData0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext executionContext
    ) throws SqlException {
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
                engine,
                compiler,
                executionContext,
                "create table y as (x union all append)",
                "y order by ts",
                "insert into x select * from append",
                "x"
        );

        assertIndexConsistency(compiler, sqlExecutionContext);
    }

    private static void testColumnTopMidAppendBlankColumn0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext executionContext
    ) throws SqlException {
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

        compiler.compile("alter table x add column v double", sqlExecutionContext);
        compiler.compile("alter table x add column v1 float", sqlExecutionContext);
        compiler.compile("alter table x add column v2 int", sqlExecutionContext);
        compiler.compile("alter table x add column v3 byte", sqlExecutionContext);
        compiler.compile("alter table x add column v4 short", sqlExecutionContext);
        compiler.compile("alter table x add column v5 boolean", sqlExecutionContext);
        compiler.compile("alter table x add column v6 date", sqlExecutionContext);
        compiler.compile("alter table x add column v7 timestamp", sqlExecutionContext);
        compiler.compile("alter table x add column v8 symbol", sqlExecutionContext);
        compiler.compile("alter table x add column v10 char", sqlExecutionContext);
        compiler.compile("alter table x add column v11 string", sqlExecutionContext);
        compiler.compile("alter table x add column v12 binary", sqlExecutionContext);
        compiler.compile("alter table x add column v9 long", sqlExecutionContext);

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
                        " rnd_char() t," +
                        //  ------------------- new columns ------------------
                        " rnd_double() v," +
                        " rnd_float() v1," +
                        " rnd_int() v2," +
                        " rnd_byte() v3," +
                        " rnd_short() v4," +
                        " rnd_boolean() v5," +
                        " rnd_date() v6," +
                        " rnd_timestamp(10,100000,356) v7," +
                        " rnd_symbol('AAA','BBB', null) v8," +
                        " rnd_char() v10," +
                        " rnd_str() v11," +
                        " rnd_bin() v12," +
                        " rnd_long() v9" +
                        " from long_sequence(100)" +
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        // create third table, which will contain both X and 1AM
        assertOutOfOrderDataConsistency(
                engine,
                compiler,
                executionContext,
                "create table y as (x union all append)",
                "y order by ts",
                "insert into x select * from append",
                "x"
        );

        assertIndexConsistency(compiler, sqlExecutionContext);
    }

    private static void testColumnTopLastAppendBlankColumn0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext executionContext
    ) throws SqlException {
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

        compiler.compile("alter table x add column v double", sqlExecutionContext);
        compiler.compile("alter table x add column v1 float", sqlExecutionContext);
        compiler.compile("alter table x add column v2 int", sqlExecutionContext);
        compiler.compile("alter table x add column v3 byte", sqlExecutionContext);
        compiler.compile("alter table x add column v4 short", sqlExecutionContext);
        compiler.compile("alter table x add column v5 boolean", sqlExecutionContext);
        compiler.compile("alter table x add column v6 date", sqlExecutionContext);
        compiler.compile("alter table x add column v7 timestamp", sqlExecutionContext);
        compiler.compile("alter table x add column v8 symbol", sqlExecutionContext);
        compiler.compile("alter table x add column v10 char", sqlExecutionContext);
        compiler.compile("alter table x add column v11 string", sqlExecutionContext);
        compiler.compile("alter table x add column v12 binary", sqlExecutionContext);
        compiler.compile("alter table x add column v9 long", sqlExecutionContext);

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
                        " timestamp_sequence(549900000000L + 1,100000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        //  ------------------- new columns ------------------
                        " rnd_double() v," +
                        " rnd_float() v1," +
                        " rnd_int() v2," +
                        " rnd_byte() v3," +
                        " rnd_short() v4," +
                        " rnd_boolean() v5," +
                        " rnd_date() v6," +
                        " rnd_timestamp(10,100000,356) v7," +
                        " rnd_symbol('AAA','BBB', null) v8," +
                        " rnd_char() v10," +
                        " rnd_str() v11," +
                        " rnd_bin() v12," +
                        " rnd_long() v9" +
                        " from long_sequence(100)" +
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        // create third table, which will contain both X and 1AM
        assertOutOfOrderDataConsistency(
                engine,
                compiler,
                executionContext,
                "create table y as (x union all append)",
                "y order by ts",
                "insert into x select * from append",
                "x"
        );

        assertIndexConsistency(compiler, sqlExecutionContext);
    }

    private static void testColumnTopMidMergeBlankColumn0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext executionContext
    ) throws SqlException {
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

        compiler.compile("alter table x add column v double", sqlExecutionContext);
        compiler.compile("alter table x add column v1 float", sqlExecutionContext);
        compiler.compile("alter table x add column v2 int", sqlExecutionContext);
        compiler.compile("alter table x add column v3 byte", sqlExecutionContext);
        compiler.compile("alter table x add column v4 short", sqlExecutionContext);
        compiler.compile("alter table x add column v5 boolean", sqlExecutionContext);
        compiler.compile("alter table x add column v6 date", sqlExecutionContext);
        compiler.compile("alter table x add column v7 timestamp", sqlExecutionContext);
        compiler.compile("alter table x add column v8 symbol index", sqlExecutionContext);
        compiler.compile("alter table x add column v10 char", sqlExecutionContext);
        compiler.compile("alter table x add column v11 string", sqlExecutionContext);
        compiler.compile("alter table x add column v12 binary", sqlExecutionContext);
        compiler.compile("alter table x add column v9 long", sqlExecutionContext);

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
                        " timestamp_sequence(518300000000L-1000L,100000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        //  ------------------- new columns ------------------
                        " rnd_double() v," +
                        " rnd_float() v1," +
                        " rnd_int() v2," +
                        " rnd_byte() v3," +
                        " rnd_short() v4," +
                        " rnd_boolean() v5," +
                        " rnd_date() v6," +
                        " rnd_timestamp(10,100000,356) v7," +
                        " rnd_symbol('AAA','BBB', null) v8," +
                        " rnd_char() v10," +
                        " rnd_str() v11," +
                        " rnd_bin() v12," +
                        " rnd_long() v9" +
                        " from long_sequence(100)" +
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        // create third table, which will contain both X and 1AM
        assertOutOfOrderDataConsistency(
                engine,
                compiler,
                executionContext,
                "create table y as (x union all append)",
                "y order by ts",
                "insert into x select * from append",
                "x"
        );

        assertIndexConsistency(compiler, sqlExecutionContext);
    }

    private static void testColumnTopNewPartitionMiddleOfTable0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext executionContext
    ) throws SqlException {
        // 1970-01-06
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

        // 1970-01-09
        compiler.compile(
                "insert into x " +
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
                        " timestamp_sequence(700000000000L,100000000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t" +
                        " from long_sequence(500)",
                sqlExecutionContext
        );

        compiler.compile("alter table x add column v double", sqlExecutionContext);
        compiler.compile("alter table x add column v1 float", sqlExecutionContext);
        compiler.compile("alter table x add column v2 int", sqlExecutionContext);
        compiler.compile("alter table x add column v3 byte", sqlExecutionContext);
        compiler.compile("alter table x add column v4 short", sqlExecutionContext);
        compiler.compile("alter table x add column v5 boolean", sqlExecutionContext);
        compiler.compile("alter table x add column v6 date", sqlExecutionContext);
        compiler.compile("alter table x add column v7 timestamp", sqlExecutionContext);
        compiler.compile("alter table x add column v8 symbol index", sqlExecutionContext);
        compiler.compile("alter table x add column v10 char", sqlExecutionContext);
        compiler.compile("alter table x add column v11 string", sqlExecutionContext);
        compiler.compile("alter table x add column v12 binary", sqlExecutionContext);
        compiler.compile("alter table x add column v9 long", sqlExecutionContext);

        // 1970-01-08
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
                        " timestamp_sequence(610000000000L,100000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        //  ------------------- new columns ------------------
                        " rnd_double() v," +
                        " rnd_float() v1," +
                        " rnd_int() v2," +
                        " rnd_byte() v3," +
                        " rnd_short() v4," +
                        " rnd_boolean() v5," +
                        " rnd_date() v6," +
                        " rnd_timestamp(10,100000,356) v7," +
                        " rnd_symbol('AAA','BBB', null) v8," +
                        " rnd_char() v10," +
                        " rnd_str() v11," +
                        " rnd_bin() v12," +
                        " rnd_long() v9" +
                        " from long_sequence(100)" +
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        // create third table, which will contain both X and 1AM
        assertOutOfOrderDataConsistency(
                engine,
                compiler,
                executionContext,
                "create table y as (x union all append)",
                "y order by ts",
                "insert into x select * from append",
                "x"
        );

        assertIndexConsistency(compiler, sqlExecutionContext);
    }

    private static void testColumnTopMidAppendColumn0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext executionContext
    ) throws SqlException, URISyntaxException {
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

        compiler.compile("alter table x add column v double", sqlExecutionContext);
        compiler.compile("alter table x add column v1 float", sqlExecutionContext);
        compiler.compile("alter table x add column v2 int", sqlExecutionContext);
        compiler.compile("alter table x add column v3 byte", sqlExecutionContext);
        compiler.compile("alter table x add column v4 short", sqlExecutionContext);
        compiler.compile("alter table x add column v5 boolean", sqlExecutionContext);
        compiler.compile("alter table x add column v6 date", sqlExecutionContext);
        compiler.compile("alter table x add column v7 timestamp", sqlExecutionContext);
        compiler.compile("alter table x add column v8 symbol", sqlExecutionContext);
        compiler.compile("alter table x add column v10 char", sqlExecutionContext);
        compiler.compile("alter table x add column v11 string", sqlExecutionContext);
        compiler.compile("alter table x add column v12 binary", sqlExecutionContext);
        compiler.compile("alter table x add column v9 long", sqlExecutionContext);

        compiler.compile(
                "insert into x " +
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
                        " timestamp_sequence(549900000000L,100000000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        // ---- new columns ----
                        " rnd_double() v," +
                        " rnd_float() v1," +
                        " rnd_int() v2," +
                        " rnd_byte() v3," +
                        " rnd_short() v4," +
                        " rnd_boolean() v5," +
                        " rnd_date() v6," +
                        " rnd_timestamp(10,100000,356) v7," +
                        " rnd_symbol('AAA','BBB', null) v8," +
                        " rnd_char() v10," +
                        " rnd_str() v11," +
                        " rnd_bin() v12," +
                        " rnd_long() v9" +
                        " from long_sequence(1000)" +
                        "",
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
                        " timestamp_sequence(604700000001,100000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        // --------- new columns -----------
                        " rnd_double() v," +
                        " rnd_float() v1," +
                        " rnd_int() v2," +
                        " rnd_byte() v3," +
                        " rnd_short() v4," +
                        " rnd_boolean() v5," +
                        " rnd_date() v6," +
                        " rnd_timestamp(10,100000,356) v7," +
                        " rnd_symbol('AAA','BBB', null) v8," +
                        " rnd_char() v10," +
                        " rnd_str() v11," +
                        " rnd_bin() v12," +
                        " rnd_long() v9" +
                        " from long_sequence(100)" +
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        assertOutOfOrderDataConsistency(
                engine,
                compiler,
                sqlExecutionContext,
                "create table y as (x union all append)",
                "insert into x select * from append",
                "/oo/testColumnTopMidAppendColumn.txt"
        );

        assertIndexConsistency(compiler, sqlExecutionContext);
    }

    private static void testColumnTopLastAppendColumn0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext executionContext
    ) throws SqlException, URISyntaxException {
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

        compiler.compile("alter table x add column v double", sqlExecutionContext);
        compiler.compile("alter table x add column v1 float", sqlExecutionContext);
        compiler.compile("alter table x add column v2 int", sqlExecutionContext);
        compiler.compile("alter table x add column v3 byte", sqlExecutionContext);
        compiler.compile("alter table x add column v4 short", sqlExecutionContext);
        compiler.compile("alter table x add column v5 boolean", sqlExecutionContext);
        compiler.compile("alter table x add column v6 date", sqlExecutionContext);
        compiler.compile("alter table x add column v7 timestamp", sqlExecutionContext);
        compiler.compile("alter table x add column v8 symbol", sqlExecutionContext);
        compiler.compile("alter table x add column v10 char", sqlExecutionContext);
        compiler.compile("alter table x add column v11 string", sqlExecutionContext);
        compiler.compile("alter table x add column v12 binary", sqlExecutionContext);
        compiler.compile("alter table x add column v9 long", sqlExecutionContext);

        compiler.compile(
                "insert into x " +
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
                        " timestamp_sequence(549900000000L,100000000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        // ---- new columns ----
                        " rnd_double() v," +
                        " rnd_float() v1," +
                        " rnd_int() v2," +
                        " rnd_byte() v3," +
                        " rnd_short() v4," +
                        " rnd_boolean() v5," +
                        " rnd_date() v6," +
                        " rnd_timestamp(10,100000,356) v7," +
                        " rnd_symbol('AAA','BBB', null) v8," +
                        " rnd_char() v10," +
                        " rnd_str() v11," +
                        " rnd_bin() v12," +
                        " rnd_long() v9" +
                        " from long_sequence(10)" +
                        "",
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
                        " timestamp_sequence(550800000000L + 1,100000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        // --------- new columns -----------
                        " rnd_double() v," +
                        " rnd_float() v1," +
                        " rnd_int() v2," +
                        " rnd_byte() v3," +
                        " rnd_short() v4," +
                        " rnd_boolean() v5," +
                        " rnd_date() v6," +
                        " rnd_timestamp(10,100000,356) v7," +
                        " rnd_symbol('AAA','BBB', null) v8," +
                        " rnd_char() v10," +
                        " rnd_str() v11," +
                        " rnd_bin() v12," +
                        " rnd_long() v9" +
                        " from long_sequence(100)" +
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        assertOutOfOrderDataConsistency(
                engine,
                compiler,
                sqlExecutionContext,
                "create table y as (x union all append)",
                "insert into x select * from append",
                "/oo/testColumnTopLastAppendColumn.txt"
        );

        assertIndexConsistency(compiler, sqlExecutionContext);
    }

    private static void testPartitionedDataOODataPbOOData0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException, URISyntaxException {
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
                engine,
                compiler,
                sqlExecutionContext,
                "create table y as (select * from x union all select * from 1am union all select * from top2)",
                "insert into x select * from (1am union all top2)",
                "/oo/testPartitionedDataOODataPbOOData.txt"
        );

        assertIndexResultAgainstFile(
                compiler,
                sqlExecutionContext,
                "/oo/testPartitionedDataOODataPbOOData_Index.txt"
        );
    }

    private void bench20(CairoEngine engine, SqlCompiler compiler, SqlExecutionContext executionContext) throws SqlException {
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
                        " timestamp_sequence(10000000000,100000L) ts," +
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
                        " timestamp_sequence(3000000000l,100000L) ts," + // mid partition for "x"
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

    private void executeWithPool(int workerCount, OutOfOrderCode runnable) throws Exception {
        executeVanilla(() -> {
            if (workerCount > 0) {
                int[] affinity = new int[workerCount];
                for (int i = 0; i < workerCount; i++) {
                    affinity[i] = -1;
                }

                WorkerPool pool = new WorkerPool(
                        new WorkerPoolAwareConfiguration() {
                            @Override
                            public int[] getWorkerAffinity() {
                                return affinity;
                            }

                            @Override
                            public int getWorkerCount() {
                                return workerCount;
                            }

                            @Override
                            public boolean haltOnError() {
                                return false;
                            }

                            @Override
                            public boolean isEnabled() {
                                return true;
                            }
                        }
                );

                pool.assignCleaner(Path.CLEANER);
                pool.assign(new OutOfOrderSortJob(engine.getMessageBus()));
                pool.assign(new OutOfOrderPartitionJob(engine.getMessageBus()));
                pool.assign(new OutOfOrderOpenColumnJob(engine.getMessageBus()));
                pool.assign(new OutOfOrderCopyJob(engine.getMessageBus()));

                OutOfOrderUtils.initBuf(pool.getWorkerCount() + 1);
                pool.start(LOG);

                try {
                    runnable.run(engine, compiler, sqlExecutionContext);
                } finally {
                    pool.halt();
                    OutOfOrderUtils.freeBuf();
                }
            } else {
                // we need to create entire engine
                final CairoConfiguration configuration = new DefaultCairoConfiguration(root) {
                    @Override
                    public int getOutOfOrderSortQueueCapacity() {
                        return 0;
                    }

                    @Override
                    public int getOutOfOrderPartitionQueueCapacity() {
                        return 0;
                    }

                    @Override
                    public int getOutOfOrderOpenColumnQueueCapacity() {
                        return 0;
                    }

                    @Override
                    public int getOutOfOrderCopyQueueCapacity() {
                        return 0;
                    }

                    @Override
                    public boolean isOutOfOrderEnabled() {
                        return true;
                    }
                };

                OutOfOrderUtils.initBuf();
                try (
                        final CairoEngine engine = new CairoEngine(configuration);
                        final SqlCompiler compiler = new SqlCompiler(engine);
                        final SqlExecutionContext sqlExecutionContext = new SqlExecutionContextImpl(engine, 1)
                ) {
                    runnable.run(engine, compiler, sqlExecutionContext);
                } finally {
                    OutOfOrderUtils.freeBuf();
                }
            }
        });
    }

    private void testBench0(CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) throws SqlException {
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

        try (
                DirectCharSink sink1 = new DirectCharSink(16*1024*1024);
                DirectCharSink sink2 = new DirectCharSink(16*1024*1024)
        ) {
            assertOutOfOrderDataConsistency(
                    engine,
                    compiler,
                    sqlExecutionContext,
                    "create table y as (x union all append)",
                    "y order by ts",
                    "insert into x select * from append",
                    "x",
                    sink1,
                    sink2
            );
        }

//        compiler.compile("insert into x select * from 1am", sqlExecutionContext);
    }

}
