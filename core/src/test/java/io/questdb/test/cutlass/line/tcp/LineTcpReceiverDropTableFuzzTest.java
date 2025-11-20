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

package io.questdb.test.cutlass.line.tcp;

import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.SCSequence;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

public class LineTcpReceiverDropTableFuzzTest extends AbstractLineTcpReceiverFuzzTest {
    private static final Log LOG = LogFactory.getLog(LineTcpReceiverDropTableFuzzTest.class);
    private SOCountDownLatch dropsDone;
    private SqlExecutionContext[] executionContexts;
    private int numOfDropThreads;
    private int numOfDrops;

    @Test
    public void testInsertDropParallel() throws Exception {
        Assume.assumeTrue(walEnabled);
        maintenanceInterval = random.nextLong(200);
        minIdleMsBeforeWriterRelease = random.nextLong(200);
        initLoadParameters(
                1 + random.nextInt(5000),
                1 + random.nextInt(10),
                1 + random.nextInt(3),
                1 + random.nextInt(4),
                1 + random.nextLong(500)
        );
        initDropParameters(random.nextInt(8), random.nextInt(4));
        initFuzzParameters(
                -1,
                -1,
                -1,
                -1,
                -1,
                false,
                false,
                false
        );
        runTest();
    }

    private void initDropParameters(int numOfDrops, int numOfDropThreads) {
        this.numOfDrops = numOfDrops;
        this.dropsDone = new SOCountDownLatch(numOfDropThreads);
        this.numOfDropThreads = numOfDropThreads;

        executionContexts = new SqlExecutionContext[numOfDropThreads];
        for (int i = 0; i < numOfDropThreads; i++) {
            executionContexts[i] = TestUtils.createSqlExecutionCtx(engine, numOfDropThreads);
        }
    }

    private CharSequence pickCreatedTableName(Rnd random) {
        int nameNo = random.nextInt(tableNames.size());
        for (CharSequence name : tableNames.keySet()) {
            if (nameNo-- == 0) {
                return name;
            }
        }
        throw new IllegalStateException();
    }

    private void startDropThread(final int threadId, SOCountDownLatch dropsDone, AtomicInteger failureCounter) {
        // use different random
        final Rnd rnd = TestUtils.generateRandom(LOG);
        new Thread(() -> {
            String sql = "";
            try {
                final SCSequence eventSubSeq = new SCSequence();
                final SqlExecutionContext executionContext = executionContexts[threadId];
                while (tableNames.isEmpty()) {
                    Os.pause();
                }

                for (int i = 0; i < numOfDrops; i++) {
                    final CharSequence tableName = pickCreatedTableName(rnd);
                    sql = "drop table if exists " + tableName;
                    execute(sql, executionContext, eventSubSeq);
                    Os.sleep(10);
                }
            } catch (Exception e) {
                e.printStackTrace();
                failureCounter.incrementAndGet();
                Assert.fail("Drop table failed [e=" + e + ", sql=" + sql + "]");
            } finally {
                Path.clearThreadLocals();
                dropsDone.countDown();
            }
        }).start();
    }

    @Override
    protected Log getLog() {
        return LOG;
    }

    @Override
    protected CharSequence pickTableName(int threadId) {
        return getTableName(pinTablesToThreads ? threadId : random.nextInt(numOfTables), false);
    }

    @Override
    protected void startThread(int threadId, Socket socket, SOCountDownLatch threadPushFinished, AtomicInteger failureCounter) {
        super.startThread(threadId, socket, threadPushFinished, failureCounter);
        while (numOfDropThreads-- > 0) {
            startDropThread(numOfDropThreads, dropsDone, failureCounter);
        }
    }

    @Override
    protected void waitDone(ObjList<Socket> sockets) throws SqlException {
        TestUtils.unchecked(() -> {
            for (int i = 0; i < numOfThreads; i++) {
                sockets.get(i).close();
                final Socket socket = newSocket();
                sockets.set(i, socket);
            }
        });

        // wait for drop threads to finish
        dropsDone.await();

        // run apply job to make sure everything has been processed
        drainWalQueue();

        // reset tables
        markTimestamp();
        clearTables();

        // ingest again
        ingest(sockets);

        // wait for ingestion to finish
        super.waitDone(sockets);
    }
}
