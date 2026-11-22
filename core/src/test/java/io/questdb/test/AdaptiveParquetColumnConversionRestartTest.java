/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | | |  _ \
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
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Before;
import org.junit.Test;

public class AdaptiveParquetColumnConversionRestartTest extends AbstractBootstrapTest {
    @Before
    public void setUp() {
        super.setUp();
        dbPath.parent().$();
    }

    @Test
    public void testWalDoubleToDecimalAfterParquetPrepassReplaysWithoutCloseEpoch() throws Exception {
        assertConversionSurvivesRestart(false);
    }

    @Test
    public void testWalDoubleToDecimalAfterParquetPrepassSurvivesCleanRestart() throws Exception {
        assertConversionSurvivesRestart(true);
    }

    private void assertConversionSurvivesRestart(boolean isFlushOnClose) throws Exception {
        TestUtils.unchecked(() -> createDummyConfiguration(
                PropertyKey.CAIRO_COMMIT_MODE.getPropertyPath() + "=adaptive",
                PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL.getPropertyPath() + "=1h",
                PropertyKey.CAIRO_ADAPTIVE_EPOCH_FLUSH_ON_CLOSE.getPropertyPath() + "=" + isFlushOnClose
        ));
        assertMemoryLeak(() -> {
            try (
                    ServerMain server = new ServerMain(getServerMainArgs());
                    SqlExecutionContext context = new SqlExecutionContextImpl(server.getEngine(), 1)
                            .with(AllowAllSecurityContext.INSTANCE)
            ) {
                server.start();
                server.getEngine().execute("CREATE TABLE x (ts TIMESTAMP, v DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL", context);
                server.getEngine().execute("""
                        INSERT INTO x VALUES
                            ('2024-01-01T00:00:00.000000Z', 1.25),
                            ('2024-01-02T00:00:00.000000Z', 2.5)
                        """, context);
                waitForWal(server, context);

                // The first day is no longer active, so the conversion actually leaves a parquet partition.
                server.getEngine().execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2024-01-01'", context);
                waitForWal(server, context);
                new QueryAssertion(server.getEngine(), context, () -> {
                }, "SELECT count() AS count FROM table_partitions('x') WHERE isParquet")
                        .noLeakCheck().noMemoryUsageCheck().noRandomAccess().expectSize().returns("count\n1\n");

                // WAL applies this at the next seqTxn. The parquet-to-native prepass must not publish
                // an epoch with the ALTER's seqTxn while _meta still describes v as DOUBLE.
                server.getEngine().execute("ALTER TABLE x ALTER COLUMN v TYPE DECIMAL(18,4)", context);
                waitForWal(server, context);
                assertResult(server, context, "SELECT type FROM table_columns('x') WHERE \"column\" = 'v'", "type\nDECIMAL(18,4)\n");
                assertValues(server, context, "SELECT v FROM x ORDER BY ts", "v\n1.2500\n2.5000\n");
            }

            // No subsequent data txn is applied before this restart. With close flushing enabled,
            // recovery must use the completed epoch; otherwise it must replay the ALTER from the
            // preceding epoch (which still references the original parquet partition).
            try (
                    ServerMain server = new ServerMain(getServerMainArgs());
                    SqlExecutionContext context = new SqlExecutionContextImpl(server.getEngine(), 1)
                            .with(AllowAllSecurityContext.INSTANCE)
            ) {
                server.start();
                waitForWal(server, context);
                assertResult(server, context, "SELECT type FROM table_columns('x') WHERE \"column\" = 'v'", "type\nDECIMAL(18,4)\n");
                assertValues(server, context, "SELECT v FROM x ORDER BY ts", "v\n1.2500\n2.5000\n");

                server.getEngine().execute("INSERT INTO x VALUES ('2024-01-02T00:00:01.000000Z', '3.75'::DECIMAL(18,4))", context);
                waitForWal(server, context);
                assertValues(server, context, "SELECT v FROM x ORDER BY ts", "v\n1.2500\n2.5000\n3.7500\n");
                server.getEngine().execute("ALTER TABLE x ADD COLUMN extra INT", context);
                waitForWal(server, context);
                assertResult(server, context, "SELECT suspended FROM wal_tables WHERE name = 'x'", "suspended\nfalse\n");
            }
        });
    }

    private static void assertResult(ServerMain server, SqlExecutionContext context, String query, String expected) throws Exception {
        new QueryAssertion(server.getEngine(), context, () -> {
        }, query).noLeakCheck().noMemoryUsageCheck().inferRandomAccess().returns(expected);
    }

    private static void assertValues(ServerMain server, SqlExecutionContext context, String query, String expected) throws Exception {
        new QueryAssertion(server.getEngine(), context, () -> {
        }, query).noLeakCheck().noMemoryUsageCheck().inferRandomAccess().expectSize().returns(expected);
    }

    private static void waitForWal(ServerMain server, SqlExecutionContext context) throws Exception {
        new QueryAssertion(server.getEngine(), context, () -> {
        }, "SELECT wait_wal_table('x')").noLeakCheck().noMemoryUsageCheck().expectSize().returns("wait_wal_table('x')\ntrue\n");
    }
}
