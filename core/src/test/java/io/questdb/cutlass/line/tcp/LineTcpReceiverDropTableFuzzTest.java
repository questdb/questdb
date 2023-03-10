/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package io.questdb.cutlass.line.tcp;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.cairo.sql.OperationFuture;
import io.questdb.griffin.*;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.SCSequence;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.std.Chars;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import static io.questdb.cairo.sql.OperationFuture.QUERY_COMPLETE;

public class LineTcpReceiverDropTableFuzzTest extends AbstractLineTcpReceiverFuzzTest {
    private static final Log LOG = LogFactory.getLog(LineTcpReceiverDropTableFuzzTest.class);
    private SqlCompiler[] compilers;
    private SOCountDownLatch dropsDone;
    private SqlExecutionContext[] executionContexts;
    private int numOfDropThreads;
    private int numOfDrops;

    public LineTcpReceiverDropTableFuzzTest(WalMode walMode) {
        super(walMode);
    }

    @Test
    public void testInsertDropParallel() throws Exception {
        Assume.assumeTrue(walEnabled);

        Rnd rnd = TestUtils.generateRandom(LOG);
        maintenanceInterval = rnd.nextLong(200);
        minIdleMsBeforeWriterRelease = rnd.nextLong(200);
        initLoadParameters(1 + rnd.nextInt(5000), 1 + rnd.nextInt(10), 1 + rnd.nextInt(3), 1 + rnd.nextInt(4), 1 + rnd.nextLong(500));
        initDropParameters(5, 3);
        initFuzzParameters(-1, -1, -1, -1, -1, false, false, false, false);
        runTest();
    }

    private void executeDrop(SqlCompiler compiler, SqlExecutionContext sqlExecutionContext, String sql, SCSequence waitSequence) throws SqlException {
        try {
            LOG.info().$(sql).$();
            final CompiledQuery cc = compiler.compile(sql, sqlExecutionContext);
            try (OperationFuture fut = cc.execute(waitSequence)) {
                if (fut.await(30 * Timestamps.SECOND_MILLIS) != QUERY_COMPLETE) {
                    throw SqlException.$(0, "drop table timeout");
                }
            }
        } catch (SqlException | CairoException ex) {
            if (!Chars.contains(ex.getFlyweightMessage(), "table does not exist")) {
                throw ex;
            }
        }
    }

    private void initDropParameters(int numOfDrops, int numOfDropThreads) {
        this.numOfDrops = numOfDrops;
        this.dropsDone = new SOCountDownLatch(numOfDropThreads);
        this.numOfDropThreads = numOfDropThreads;

        compilers = new SqlCompiler[numOfDropThreads];
        executionContexts = new SqlExecutionContext[numOfDropThreads];
        for (int i = 0; i < numOfDropThreads; i++) {
            compilers[i] = new SqlCompiler(engine, null, null);
            SqlExecutionContextImpl sqlExecutionContext = new SqlExecutionContextImpl(engine, numOfDropThreads);
            sqlExecutionContext.with(AllowAllCairoSecurityContext.INSTANCE, null, null);
            executionContexts[i] = sqlExecutionContext;
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

    private void startDropThread(final int threadId, SOCountDownLatch dropsDone) {
        // use different random
        final Rnd rnd = TestUtils.generateRandom(LOG);
        new Thread(() -> {
            String sql = "";
            try {
                final SCSequence waitSequence = new SCSequence();
                final SqlCompiler compiler = compilers[threadId];
                final SqlExecutionContext executionContext = executionContexts[threadId];
                while (tableNames.size() == 0) {
                    Os.pause();
                }

                for (int i = 0; i < numOfDrops; i++) {
                    final CharSequence tableName = pickCreatedTableName(rnd);
                    sql = "drop table " + tableName;
                    executeDrop(compiler, executionContext, sql, waitSequence);
                    Os.sleep(10);
                }
            } catch (Exception e) {
                e.printStackTrace();
                Assert.fail("Drop table failed [e=" + e + ", sql=" + sql + "]");
                throw new RuntimeException(e);
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
    protected void startThread(int threadId, Socket socket, SOCountDownLatch threadPushFinished) {
        super.startThread(threadId, socket, threadPushFinished);
        while (numOfDropThreads-- > 0) {
            startDropThread(numOfDropThreads, dropsDone);
        }
    }

    @Override
    protected void waitDone(ObjList<Socket> sockets) {
        for (int i = 0; i < numOfThreads; i++) {
            try {
                sockets.get(i).close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            final Socket socket = newSocket();
            sockets.set(i, socket);
        }

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
