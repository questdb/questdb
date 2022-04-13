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

package io.questdb.cutlass.line.tcp;

import io.questdb.cairo.sql.ReaderOutOfDateException;
import io.questdb.cutlass.line.tcp.load.LineData;
import io.questdb.cutlass.line.tcp.load.TableData;
import io.questdb.griffin.*;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.SCSequence;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.std.Chars;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.ConcurrentLinkedQueue;

public class LineTcpReceiverUpdateFuzzTest extends AbstractLineTcpReceiverFuzzTest {

    private static final Log LOG = LogFactory.getLog(LineTcpReceiverUpdateFuzzTest.class);
    private final ConcurrentLinkedQueue<String> updatesSql = new ConcurrentLinkedQueue<>();
    private int numOfUpdates;
    private SOCountDownLatch updatesDone;
    private int numOfUpdateThreads;
    private SqlCompiler[] compilers;
    private SqlExecutionContext[] executionContexts;

    @BeforeClass
    public static void setUpStatic() {
        writerCommandQueueCapacity = 1024;
        AbstractGriffinTest.setUpStatic();
    }

    @Override
    @Before
    public void setUp() {
        writerCommandQueueCapacity = 1024;
        super.setUp();
    }

    @Test
    public void testInsertUpdateParallel() throws Exception {
        initLoadParameters(50, 2, 2, 5, 75);
        initUpdateParameters(5, 3);
        initFuzzParameters(-1, -1, -1, -1, -1, false, false, false, false);
        runTest();
    }

    @Test
    public void testInsertUpdateSequencial() throws Exception {
        initLoadParameters(50, 2, 2, 5, 75);
        initUpdateParameters(10, 1);
        initFuzzParameters(-1, -1, -1, -1, -1, false, false, false, false);
        runTest();
    }

    private void executeUpdate(SqlCompiler compiler, SqlExecutionContext sqlExecutionContext, String sql, SCSequence waitSequence) throws SqlException {
        while (true) {
            try {
                CompiledQuery cc;
                cc = compiler.compile(sql, sqlExecutionContext);
                cc.execute(waitSequence).await(10 * Timestamps.SECOND_MICROS);
                return;
            } catch (ReaderOutOfDateException ex) {
                // retry, e.g. continue
            } catch (SqlException ex) {
                if (Chars.contains(ex.getFlyweightMessage(), "cached query plan cannot be used because table schema has changed")) {
                    continue;
                }
                throw ex;
            }
        }
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
    protected void startThread(int threadId, SOCountDownLatch threadPushFinished) {
        super.startThread(threadId, threadPushFinished);
        while (this.numOfUpdateThreads-- > 0) {
            startUpdateThread(this.numOfUpdateThreads, updatesDone);
        }
    }

    @Override
    protected void waitDone() {
        updatesDone.await();

        SCSequence waitSequence = new SCSequence();
        SqlCompiler compiler = compilers[0];
        SqlExecutionContext executionContext = executionContexts[0];
        for (String sql : updatesSql) {
            try {
                executeUpdate(compiler, executionContext, sql, waitSequence);
            } catch (SqlException e) {
                LOG.error().$("update failed").$((Throwable) e).$();
            }
        }
    }

    private void initUpdateParameters(int numOfUpdates, int numOfThreads) {
        this.numOfUpdates = numOfUpdates;
        this.updatesSql.clear();
        this.updatesDone = new SOCountDownLatch(numOfThreads);
        this.numOfUpdateThreads = numOfThreads;
        compilers = new SqlCompiler[numOfThreads];
        executionContexts = new SqlExecutionContext[numOfThreads];
        for (int i = 0; i < numOfThreads; i++) {
            compilers[i] = new SqlCompiler(engine, null, null);
            executionContexts[i] = new SqlExecutionContextImpl(engine, numOfThreads);
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

    private void startUpdateThread(final int threadId, SOCountDownLatch updatesDone) {
        Rnd rnd = TestUtils.generateRandom();
        new Thread(() -> {
            String sql = "";
            try {
                SCSequence waitSequence = new SCSequence();
                SqlCompiler compiler = compilers[threadId];
                SqlExecutionContext executionContext = executionContexts[threadId];
                while (tableNames.size() == 0) {
                    Os.pause();
                }

                for (int j = 0; j < numOfUpdates; j++) {
                    final CharSequence tableName = pickCreatedTableName(rnd);
                    final TableData table = tables.get(tableName);
                    int lineNo = rnd.nextInt(table.size());
                    LineData line = table.getLine(lineNo);
                    sql = line.generateRandomUpdate(tableName, rnd, colTypes);
                    executeUpdate(compiler, executionContext, sql, waitSequence);
                    this.updatesSql.add(sql);
                }
            } catch (Exception e) {
                Assert.fail("Data sending failed [e=" + e + ", sql=" + sql + "]");
                throw new RuntimeException(e);
            } finally {
                updatesDone.countDown();
            }
        }).start();
    }
}
