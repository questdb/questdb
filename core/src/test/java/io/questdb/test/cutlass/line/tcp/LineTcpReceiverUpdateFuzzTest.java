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

package io.questdb.test.cutlass.line.tcp;

import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableReaderMetadata;
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
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cutlass.line.tcp.load.LineData;
import io.questdb.test.cutlass.line.tcp.load.TableData;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class LineTcpReceiverUpdateFuzzTest extends AbstractLineTcpReceiverFuzzTest {
    private static final Log LOG = LogFactory.getLog(LineTcpReceiverUpdateFuzzTest.class);
    private final ConcurrentLinkedQueue<String> updateSqlQueue = new ConcurrentLinkedQueue<>();
    private SqlExecutionContext[] executionContexts;
    private int numOfUpdateThreads;
    private int numOfUpdates;
    private SOCountDownLatch updatesDone;

    public LineTcpReceiverUpdateFuzzTest(WalMode walMode) {
        super(walMode);
    }

    @BeforeClass
    public static void setUpStatic() throws Exception {
        writerCommandQueueCapacity = 1024;
        AbstractCairoTest.setUpStatic();
    }

    @Override
    @Before
    public void setUp() {
        writerCommandQueueCapacity = 1024;
        writerAsyncCommandBusyWaitTimeout = 5000;
        super.setUp();
        node1.getConfigurationOverrides().setWriterAsyncCommandBusyWaitTimeout(5000);
        node1.getConfigurationOverrides().setSpinLockTimeout(5000);
    }

    @Test
    public void testInsertUpdateParallel() throws Exception {
        initLoadParameters(50, 2, 2, 5, 75);
        initUpdateParameters(5, 3);
        initFuzzParameters(
                -1,
                -1,
                -1,
                -1,
                -1,
                false,
                false,
                false,
                false
        );
        runTest();
    }

    @Test
    public void testInsertUpdateSequential() throws Exception {
        initLoadParameters(50, 2, 2, 5, 75);
        initUpdateParameters(10, 1);
        initFuzzParameters(
                -1,
                -1,
                -1,
                -1,
                -1,
                false,
                false,
                false,
                false
        );
        runTest();
    }

    @Test
    public void testInsertUpdateWithColumnAdds() throws Exception {
        initLoadParameters(50, 1, 2, 3, 75);
        initUpdateParameters(15, 1);
        initFuzzParameters(
                -1,
                3,
                2,
                2,
                -1,
                false,
                false,
                false,
                false
        );
        runTest();
    }

    private List<ColumnNameType> getMetaData(Map<CharSequence, ArrayList<ColumnNameType>> columnsCache, CharSequence tableName) {
        if (columnsCache.containsKey(tableName)) {
            return columnsCache.get(tableName);
        }
        try (TableReader reader = getReader(tableName)) {
            final TableReaderMetadata metadata = reader.getMetadata();
            final ArrayList<ColumnNameType> columns = new ArrayList<>();
            for (int i = metadata.getColumnCount() - 1; i > -1L; i--) {
                if (i != metadata.getTimestampIndex()) {
                    columns.add(new ColumnNameType(metadata.getColumnName(i), metadata.getColumnType(i)));
                }
            }
            columnsCache.put(tableName, columns);
            return columns;
        }
    }

    private void initUpdateParameters(int numOfUpdates, int numOfUpdateThreads) {
        this.numOfUpdates = numOfUpdates;
        this.updateSqlQueue.clear();
        this.updatesDone = new SOCountDownLatch(numOfUpdateThreads);
        this.numOfUpdateThreads = numOfUpdateThreads;

        executionContexts = new SqlExecutionContext[numOfUpdateThreads];
        for (int i = 0; i < numOfUpdateThreads; i++) {
            executionContexts[i] = TestUtils.createSqlExecutionCtx(engine, numOfUpdateThreads);
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

    private void startUpdateThread(final int threadId, SOCountDownLatch updatesDone, AtomicInteger failureCounter) {
        // use different random
        System.out.println("thread random");
        final Rnd rnd = TestUtils.generateRandom(LOG);
        new Thread(() -> {
            String updateSql = "";
            try {
                final Map<CharSequence, ArrayList<ColumnNameType>> columnsCache = new HashMap<>();
                final SCSequence eventSubSeq = new SCSequence();
                final SqlExecutionContext executionContext = executionContexts[threadId];
                while (tableNames.isEmpty()) {
                    Os.pause();
                }

                for (int j = 0; j < numOfUpdates; j++) {
                    final CharSequence tableName = pickCreatedTableName(rnd);
                    final List<ColumnNameType> columns = getMetaData(columnsCache, tableName);
                    final TableData table = tables.get(tableName);
                    final LineData line = table.getRandomValidLine(rnd);

                    Collections.shuffle(columns);
                    updateSql = line.generateRandomUpdate(tableName, columns, rnd);
                    update(updateSql, executionContext, eventSubSeq);
                    updateSqlQueue.add(updateSql);
                }
            } catch (Exception e) {
                Assert.fail("Update failed [e=" + e + ", updateSql=" + updateSql + "]");
                failureCounter.incrementAndGet();
            } finally {
                Path.clearThreadLocals();
                updatesDone.countDown();
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
        while (numOfUpdateThreads-- > 0) {
            startUpdateThread(numOfUpdateThreads, updatesDone, failureCounter);
        }
    }

    @Override
    protected void waitDone(ObjList<Socket> sockets) throws SqlException {
        // wait for update threads to finish
        updatesDone.await();

        // wait for ingestion to finish
        super.waitDone(sockets);

        // repeat all updates after all lines are guaranteed to be landed in the tables
        for (String updateSql : updateSqlQueue) {
            update(updateSql);
        }
        mayDrainWalQueue();
    }
}
