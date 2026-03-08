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

package io.questdb.test;

import io.questdb.PropertyKey;
import io.questdb.ServerMain;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.OperationFuture;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.log.LogFactory;
import io.questdb.std.Misc;
import io.questdb.std.str.StringSink;
import io.questdb.test.cairo.TableModel;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import static io.questdb.test.tools.TestUtils.insertFromSelectPopulateTableStmt;

/**
 * Here we test edge cases around worker thread pool sizes and worker ids.
 * PGWire pool size is intentionally set to a higher value than the shared pool.
 */
public class ServerMainVectorGroupByTest extends AbstractBootstrapTest {

    private static final int PG_WIRE_POOL_SIZE = 4;
    private static final int SHARED_POOL_SIZE = 1;
    private static final int pgPortDelta = 17;
    private static final int pgPort = PG_PORT + pgPortDelta;

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
                PropertyKey.PG_WORKER_COUNT.getPropertyPath() + "=" + PG_WIRE_POOL_SIZE,
                PropertyKey.SHARED_WORKER_COUNT.getPropertyPath() + "=" + SHARED_POOL_SIZE,
                // Set vector aggregate queue to a small size to have better chances of work stealing.
                PropertyKey.CAIRO_VECTOR_AGGREGATE_QUEUE_CAPACITY.getPropertyPath() + "=2"
        ));
    }

    @Test
    public void testKeyedGroupByDoesNotFail() throws Exception {
        String tableName = testName.getMethodName();
        assertMemoryLeak(() -> {
            try (
                    ServerMain qdb = new ServerMain(getServerMainArgs());
                    SqlCompiler compiler = qdb.getEngine().getSqlCompiler()
            ) {
                CairoEngine engine1 = qdb.getEngine();
                try (SqlExecutionContext context = TestUtils.createSqlExecutionCtx(engine1)
                ) {
                    qdb.start();
                    CairoEngine engine = qdb.getEngine();
                    CairoConfiguration cairoConfig = qdb.getConfiguration().getCairoConfiguration();

                    Assert.assertEquals(SHARED_POOL_SIZE, qdb.getConfiguration().getSharedWorkerPoolNetworkConfiguration().getWorkerCount());
                    Assert.assertEquals(PG_WIRE_POOL_SIZE, qdb.getConfiguration().getPGWireConfiguration().getWorkerCount());

                    TableToken tableToken = createPopulateTable(cairoConfig, engine, compiler, context, tableName);
                    assertQueryDoesNotFail("select max(l), s from " + tableToken.getTableName(), 5);
                }
            }
        });
    }

    @Test
    public void testNonKeyedGroupByDoesNotFail() throws Exception {
        String tableName = testName.getMethodName();
        assertMemoryLeak(() -> {
            try (
                    ServerMain qdb = new ServerMain(getServerMainArgs());
                    SqlCompiler compiler = qdb.getEngine().getSqlCompiler()
            ) {
                CairoEngine engine1 = qdb.getEngine();
                try (SqlExecutionContext context = TestUtils.createSqlExecutionCtx(engine1)
                ) {
                    qdb.start();
                    CairoEngine engine = qdb.getEngine();
                    CairoConfiguration cairoConfig = qdb.getConfiguration().getCairoConfiguration();

                    Assert.assertEquals(SHARED_POOL_SIZE, qdb.getConfiguration().getSharedWorkerPoolNetworkConfiguration().getWorkerCount());
                    Assert.assertEquals(PG_WIRE_POOL_SIZE, qdb.getConfiguration().getPGWireConfiguration().getWorkerCount());

                    TableToken tableToken = createPopulateTable(cairoConfig, engine, compiler, context, tableName);
                    assertQueryDoesNotFail("select max(l) from " + tableToken.getTableName(), 1);
                }
            }
        });
    }

    private static void assertQueryDoesNotFail(String query, int expectedRows) throws Exception {
        try (
                Connection conn = DriverManager.getConnection(getPgConnectionUri(pgPort), PG_CONNECTION_PROPERTIES);
                PreparedStatement stmt = conn.prepareStatement(query);
                ResultSet result = stmt.executeQuery()
        ) {
            int actualRows = 0;
            while (result.next()) {
                actualRows++;
            }
            Assert.assertEquals(expectedRows, actualRows);
        }
    }

    private TableToken createPopulateTable(
            CairoConfiguration cairoConfig,
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext context,
            String tableName
    ) throws Exception {
        StringSink sink = Misc.getThreadLocalSink();
        sink.put("CREATE TABLE ");
        sink.put(tableName).put('(');
        sink.put(" l LONG,");
        sink.put(" s SYMBOL,");
        sink.put(" ts TIMESTAMP");
        sink.put(") TIMESTAMP(ts) PARTITION BY DAY");
        engine.execute(sink, context);
        TableModel tableModel = new TableModel(cairoConfig, tableName, PartitionBy.DAY)
                .col("l", ColumnType.LONG)
                .col("s", ColumnType.SYMBOL)
                .timestamp("ts");
        try (
                OperationFuture op = compiler.compile(insertFromSelectPopulateTableStmt(tableModel, 10000, "2020-01-01", 100), context).execute(null)
        ) {
            op.await();
        }
        return engine.verifyTableName(tableName);
    }

    static {
        // log is needed to greedily allocate logger infra and
        // exclude it from leak detector
        LogFactory.getLog(ServerMainVectorGroupByTest.class);
    }
}
