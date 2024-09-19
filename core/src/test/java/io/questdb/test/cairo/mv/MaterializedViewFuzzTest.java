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

package io.questdb.test.cairo.mv;

import io.questdb.cairo.TableToken;
import io.questdb.cairo.mv.MaterializedViewDefinition;
import io.questdb.cairo.mv.MaterializedViewRefreshJob;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.std.str.Path;
import io.questdb.test.fuzz.FuzzTransaction;
import io.questdb.test.griffin.wal.AbstractFuzzTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

public class MaterializedViewFuzzTest extends AbstractFuzzTest {
    @Test
    public void testFuzz() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            String mvName = testName.getMethodName() + "_mv";
            TableToken baseToken = fuzzer.createInitialTable(tableName, true);
            Rnd rnd = fuzzer.generateRandom(LOG);

            String viewSql = "select min(c3), max(c3), ts from  " + tableName + " sample by 1h";
            createMatView(
                    baseToken,
                    viewSql,
                    mvName, "ts"
            );

            AtomicBoolean stop = new AtomicBoolean();
            Thread refreshJob = startRefreshJob(stop, rnd);

            fuzzer.setFuzzCounts(
                    rnd.nextBoolean(),
                    rnd.nextInt(2_000_000),
                    rnd.nextInt(1000),
                    rnd.nextInt(3),
                    rnd.nextInt(5),
                    rnd.nextInt(1000),
                    rnd.nextInt(1_000_000),
                    5 + rnd.nextInt(10)
            );

            // Easy, not column manupulations
            fuzzer.setFuzzProbabilities(
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0.5,
                    0,
                    0.5,
                    0,
                    0
            );

            ObjList<FuzzTransaction> transactions = fuzzer.generateTransactions(tableName, rnd);
            ObjList<ObjList<FuzzTransaction>> fuzzTransactions = new ObjList<>();
            fuzzTransactions.add(transactions);
            fuzzer.applyManyWalParallel(
                    fuzzTransactions,
                    rnd,
                    tableName,
                    false,
                    true
            );

            stop.set(true);
            refreshJob.join();
            drainWalQueue();

            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                TestUtils.assertSqlCursors(
                        compiler,
                        sqlExecutionContext,
                        viewSql,
                        mvName,
                        LOG
                );
            }
        });
    }

    private static void createMatView(TableToken baseToken, String viewSql, String mvName, String upsertKeys) throws SqlException {
        ddl("create table " + mvName + " as (" +
                "SELECT * FROM (" + viewSql + ") WHERE 1 = 2" +
                ") timestamp(ts) partition by DAY WAL dedup upsert keys(" + upsertKeys + ")"
        );

        MaterializedViewDefinition viewDefinition = new MaterializedViewDefinition();

        viewDefinition.setParentTableName(baseToken.getTableName());
        viewDefinition.setViewSql(viewSql);
        viewDefinition.setSampleByPeriodMicros(Timestamps.HOUR_MICROS);
        viewDefinition.setTableToken(engine.verifyTableName(mvName));
        engine.getMaterializedViewGraph().upsertView(baseToken, viewDefinition);
    }

    private Thread startRefreshJob(AtomicBoolean stop, Rnd outsideRnd) {
        Rnd rnd = new Rnd(outsideRnd.nextLong(), outsideRnd.nextLong());
        Thread th = new Thread(() -> {
            try {
                MaterializedViewRefreshJob refreshJob = new MaterializedViewRefreshJob(engine);
                while (!stop.get()) {
                    refreshJob.run(0);
                    Os.sleep(rnd.nextInt(1000));
                }

                // Run one final time before stopping
                refreshJob.run(0);
            } finally {
                Path.clearThreadLocals();
                LOG.info().$("Refresh job stopped").$();
            }
        });
        th.start();
        return th;
    }
}
