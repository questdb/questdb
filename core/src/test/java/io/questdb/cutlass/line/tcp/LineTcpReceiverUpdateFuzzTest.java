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

import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableReaderMetadata;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.cairo.sql.OperationFuture;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.cutlass.line.tcp.load.LineData;
import io.questdb.cutlass.line.tcp.load.TableData;
import io.questdb.griffin.*;
import io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl;
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
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

import static io.questdb.cairo.sql.OperationFuture.QUERY_COMPLETE;

public class LineTcpReceiverUpdateFuzzTest extends AbstractLineTcpReceiverFuzzTest {
    private static final Log LOG = LogFactory.getLog(LineTcpReceiverUpdateFuzzTest.class);
    private final ConcurrentLinkedQueue<String> updatesQueue = new ConcurrentLinkedQueue<>();
    private SqlCompiler[] compilers;
    private SqlExecutionContext[] executionContexts;
    private int numOfUpdateThreads;
    private int numOfUpdates;
    private SOCountDownLatch updatesDone;

    public LineTcpReceiverUpdateFuzzTest(WalMode walMode) {
        super(walMode);
    }

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
    public void testInsertUpdateSequential() throws Exception {
        initLoadParameters(50, 2, 2, 5, 75);
        initUpdateParameters(10, 1);
        initFuzzParameters(-1, -1, -1, -1, -1, false, false, false, false);
        runTest();
    }

    @Test
    public void testInsertUpdateWithColumnAdds() throws Exception {
        initLoadParameters(50, 1, 2, 3, 75);
        initUpdateParameters(15, 1);
        initFuzzParameters(-1, 3, 2, 2, -1, false, false, false, false);
        runTest();
    }

    private void executeUpdate(SqlCompiler compiler, SqlExecutionContext sqlExecutionContext, String sql, SCSequence waitSequence) {
        while (true) {
            try {
                LOG.info().$(sql).$();
                final CompiledQuery cc = compiler.compile(sql, sqlExecutionContext);
                try (OperationFuture fut = cc.execute(waitSequence)) {
                    if (fut.await(30 * Timestamps.SECOND_MILLIS) != QUERY_COMPLETE) {
                        throw SqlException.$(0, "update query timeout");
                    }
                }
                return;
            } catch (TableReferenceOutOfDateException ex) {
                // retry, e.g. continue
            } catch (SqlException ex) {
                if (Chars.contains(ex.getFlyweightMessage(), "cached query plan cannot be used because table schema has changed")) {
                    continue;
                }
                throw new RuntimeException(ex);
            }
        }
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
        this.updatesQueue.clear();
        this.updatesDone = new SOCountDownLatch(numOfUpdateThreads);
        this.numOfUpdateThreads = numOfUpdateThreads;

        compilers = new SqlCompiler[numOfUpdateThreads];
        executionContexts = new SqlExecutionContext[numOfUpdateThreads];
        for (int i = 0; i < numOfUpdateThreads; i++) {
            compilers[i] = new SqlCompiler(engine, null, null);
            BindVariableServiceImpl bindService = new BindVariableServiceImpl(configuration);
            SqlExecutionContextImpl sqlExecutionContext = new SqlExecutionContextImpl(engine, numOfUpdateThreads);
            sqlExecutionContext.with(AllowAllCairoSecurityContext.INSTANCE, bindService, null);
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

    private void startUpdateThread(final int threadId, SOCountDownLatch updatesDone) {
        // use different random
        System.out.println("thread random");
        final Rnd rnd = TestUtils.generateRandom(LOG);
        new Thread(() -> {
            String sql = "";
            try {
                final Map<CharSequence, ArrayList<ColumnNameType>> columnsCache = new HashMap<>();
                final SCSequence waitSequence = new SCSequence();
                final SqlCompiler compiler = compilers[threadId];
                final SqlExecutionContext executionContext = executionContexts[threadId];
                while (tableNames.size() == 0) {
                    Os.pause();
                }

                for (int j = 0; j < numOfUpdates; j++) {
                    final CharSequence tableName = pickCreatedTableName(rnd);
                    final List<ColumnNameType> columns = getMetaData(columnsCache, tableName);
                    final TableData table = tables.get(tableName);
                    final LineData line = table.getRandomValidLine(rnd);

                    Collections.shuffle(columns);
                    sql = line.generateRandomUpdate(tableName, columns, rnd);
                    executeUpdate(compiler, executionContext, sql, waitSequence);
                    updatesQueue.add(sql);
                }
            } catch (Exception e) {
                Assert.fail("Update failed [e=" + e + ", sql=" + sql + "]");
                throw new RuntimeException(e);
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
    protected void startThread(int threadId, Socket socket, SOCountDownLatch threadPushFinished) {
        super.startThread(threadId, socket, threadPushFinished);
        while (numOfUpdateThreads-- > 0) {
            startUpdateThread(numOfUpdateThreads, updatesDone);
        }
    }

    @Override
    protected void waitDone(ObjList<Socket> sockets) {
        // wait for update threads to finish
        updatesDone.await();

        // wait for ingestion to finish
        super.waitDone(sockets);

        // repeat all updates after all lines are guaranteed to be landed in the tables
        final SqlCompiler compiler = compilers[0];
        final SqlExecutionContext executionContext = executionContexts[0];
        for (String sql : updatesQueue) {
            executeUpdate(compiler, executionContext, sql, null);
        }
        mayDrainWalQueue();
    }
}
