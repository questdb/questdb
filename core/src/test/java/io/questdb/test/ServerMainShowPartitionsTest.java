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

package io.questdb.test;

import io.questdb.PropertyKey;
import io.questdb.ServerMain;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.InsertOperation;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.QueryBuilder;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.log.LogFactory;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;
import io.questdb.test.cairo.TableModel;
import io.questdb.test.tools.TestUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static io.questdb.test.griffin.ShowPartitionsTest.testTableName;
import static io.questdb.test.tools.TestUtils.*;

@RunWith(Parameterized.class)
public class ServerMainShowPartitionsTest extends AbstractBootstrapTest {
    private static final String EXPECTED = """
            index\tpartitionBy\tname\tminTimestamp\tmaxTimestamp\tnumRows\tdiskSize\tdiskSizeHuman\treadOnly\tactive\tattached\tdetached\tattachable\tisParquet\tparquetFileSize
            0\tDAY\t2023-01-01\t2023-01-01T00:00:00.950399Z\t2023-01-01T23:59:59.822691Z\t90909\tSIZE\tHUMAN\tfalse\tfalse\ttrue\tfalse\tfalse\tfalse\t-1
            1\tDAY\t2023-01-02\t2023-01-02T00:00:00.773090Z\t2023-01-02T23:59:59.645382Z\t90909\tSIZE\tHUMAN\tfalse\tfalse\ttrue\tfalse\tfalse\tfalse\t-1
            2\tDAY\t2023-01-03\t2023-01-03T00:00:00.595781Z\t2023-01-03T23:59:59.468073Z\t90909\tSIZE\tHUMAN\tfalse\tfalse\ttrue\tfalse\tfalse\tfalse\t-1
            3\tDAY\t2023-01-04\t2023-01-04T00:00:00.418472Z\t2023-01-04T23:59:59.290764Z\t90909\tSIZE\tHUMAN\tfalse\tfalse\ttrue\tfalse\tfalse\tfalse\t-1
            4\tDAY\t2023-01-05\t2023-01-05T00:00:00.241163Z\t2023-01-05T23:59:59.113455Z\t90909\tSIZE\tHUMAN\tfalse\tfalse\ttrue\tfalse\tfalse\tfalse\t-1
            5\tDAY\t2023-01-06\t2023-01-06T00:00:00.063854Z\t2023-01-06T23:59:59.886545Z\t90910\tSIZE\tHUMAN\tfalse\tfalse\ttrue\tfalse\tfalse\tfalse\t-1
            6\tDAY\t2023-01-07\t2023-01-07T00:00:00.836944Z\t2023-01-07T23:59:59.709236Z\t90909\tSIZE\tHUMAN\tfalse\tfalse\ttrue\tfalse\tfalse\tfalse\t-1
            7\tDAY\t2023-01-08\t2023-01-08T00:00:00.659635Z\t2023-01-08T23:59:59.531927Z\t90909\tSIZE\tHUMAN\tfalse\tfalse\ttrue\tfalse\tfalse\tfalse\t-1
            8\tDAY\t2023-01-09\t2023-01-09T00:00:00.482326Z\t2023-01-09T23:59:59.354618Z\t90909\tSIZE\tHUMAN\tfalse\tfalse\ttrue\tfalse\tfalse\tfalse\t-1
            9\tDAY\t2023-01-10\t2023-01-10T00:00:00.305017Z\t2023-01-10T23:59:59.177309Z\t90909\tSIZE\tHUMAN\tfalse\tfalse\ttrue\tfalse\tfalse\tfalse\t-1
            10\tDAY\t2023-01-11\t2023-01-11T00:00:00.127708Z\t2023-01-11T23:59:59.000000Z\t90909\tSIZE\tHUMAN\tfalse\ttrue\ttrue\tfalse\tfalse\tfalse\t-1
            """;

    private static final String firstPartitionName = "2023-01-01";
    private static final int partitionCount = 11;
    private static final int pgPortDelta = 11;
    private static final int pgPort = PG_PORT + pgPortDelta;
    private final boolean isWal;
    private final String tableNameSuffix;

    public ServerMainShowPartitionsTest(AbstractCairoTest.WalMode walMode, String tableNameSuffix) {
        isWal = (AbstractCairoTest.WalMode.WITH_WAL == walMode);
        this.tableNameSuffix = tableNameSuffix;
    }

    @Parameterized.Parameters(name = "{0}-{1}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {AbstractCairoTest.WalMode.NO_WAL, null},
                {AbstractCairoTest.WalMode.NO_WAL, "テンション"},
                {AbstractCairoTest.WalMode.WITH_WAL, null},
                {AbstractCairoTest.WalMode.WITH_WAL, "テンション"}
        });
    }

    @Override
    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration(
                HTTP_PORT + pgPortDelta,
                HTTP_MIN_PORT + pgPortDelta,
                pgPort,
                ILP_PORT + pgPortDelta,
                root,
                PropertyKey.CAIRO_WAL_SUPPORTED.getPropertyPath() + "=true"
        ));
    }

    @Test
    public void testServerMainShowPartitions() throws Exception {
        String tableName = testTableName(testName.getMethodName(), tableNameSuffix);
        assertMemoryLeak(() -> {
            try (
                    ServerMain qdb = new ServerMain(getServerMainArgs());
                    SqlCompiler defaultCompiler = qdb.getEngine().getSqlCompiler();
                    SqlExecutionContext defaultContext = createSqlExecutionCtx(qdb.getEngine())
            ) {
                qdb.start();
                CairoEngine engine = qdb.getEngine();
                CairoConfiguration cairoConfig = qdb.getConfiguration().getCairoConfiguration();

                TableToken tableToken = createPopulateTable(cairoConfig, engine, defaultCompiler, defaultContext, tableName);
                engine.awaitTable(tableName, 30, TimeUnit.SECONDS);

                String finallyExpected = replaceSizeToMatchOS(EXPECTED, dbPath, tableToken.getTableName(), engine, new StringSink());
                assertShowPartitions(finallyExpected, tableToken, defaultCompiler, defaultContext);

                int numThreads = 5;
                SOCountDownLatch completed = new SOCountDownLatch(numThreads);
                AtomicReference<Throwable> errors = new AtomicReference<>();
                ObjList<SqlCompiler> compilers = new ObjList<>(numThreads);
                ObjList<SqlExecutionContext> contexts = new ObjList<>(numThreads);
                for (int i = 0; i < numThreads; i++) {
                    SqlCompiler compiler = qdb.getEngine().getSqlCompiler();
                    SqlExecutionContext context = createSqlExecutionCtx(qdb.getEngine());
                    compilers.add(compiler);
                    contexts.add(context);
                    new Thread(() -> {
                        try {
                            assertShowPartitions(finallyExpected, tableToken, compiler, context);
                        } catch (Throwable err) {
                            errors.compareAndSet(null, err);
                        } finally {
                            completed.countDown();
                        }
                    }).start();
                }
                if (!completed.await(TimeUnit.MINUTES.toNanos(1))) {
                    errors.compareAndSet(null, new AssertionError("Timed out waiting for threads to complete"));
                    TestListener.dumpThreadStacks();
                }
                dropTable(defaultContext, tableToken);
                Misc.freeObjListAndClear(compilers);
                Misc.freeObjListAndClear(contexts);

                // fail on first error found
                Throwable firstError = errors.get();
                if (firstError != null) {
                    throw new AssertionError(firstError);
                }
            }
        });
    }

    private static void assertShowPartitions(
            String finallyExpected,
            TableToken tableToken,
            SqlCompiler compiler,
            SqlExecutionContext context
    ) throws SqlException {
        try (
                RecordCursorFactory factory = compiler.compile("SHOW PARTITIONS FROM " + tableToken.getTableName(), context).getRecordCursorFactory();
                RecordCursor cursor0 = factory.getCursor(context);
                RecordCursor cursor1 = factory.getCursor(context)
        ) {
            RecordMetadata meta = factory.getMetadata();
            StringSink sink = Misc.getThreadLocalSink();
            LongList rows = new LongList();
            for (int j = 0; j < 5; j++) {
                AbstractCairoTest.assertCursor(finallyExpected, false, true, false, cursor0, meta, sink, rows, false);
                cursor0.toTop();
                AbstractCairoTest.assertCursor(finallyExpected, false, true, false, cursor1, meta, sink, rows, false);
                cursor1.toTop();
            }
        }
    }

    private TableToken createPopulateTable(
            CairoConfiguration cairoConfig,
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext context,
            String tableName
    ) throws Exception {
        QueryBuilder qb = compiler.query();
        qb.$("CREATE TABLE ").$(tableName).$('(');
        qb
                .$("  investmentMill LONG,")
                .$("  ticketThous INT,")
                .$("broker SYMBOL,")
                .$("ts TIMESTAMP")
                .$(") TIMESTAMP(ts) PARTITION BY DAY");

        if (isWal) {
            qb.$(" WAL");
        }
        qb.createTable(context);

        TableModel tableModel = new TableModel(cairoConfig, tableName, PartitionBy.DAY)
                .col("investmentMill", ColumnType.LONG)
                .col("ticketThous", ColumnType.INT)
                .col("broker", ColumnType.SYMBOL).symbolCapacity(32)
                .timestamp("ts");
        CharSequence insert = insertFromSelectPopulateTableStmt(tableModel, 1000000, firstPartitionName, partitionCount);
        try (InsertOperation op = compiler.compile(insert, context).popInsertOperation()) {
            op.execute(context);
        }
        return engine.verifyTableName(tableName);
    }

    static {
        // log is needed to greedily allocate logger
        // infra and exclude it from leak detector
        LogFactory.getLog(ServerMainShowPartitionsTest.class);
    }
}
