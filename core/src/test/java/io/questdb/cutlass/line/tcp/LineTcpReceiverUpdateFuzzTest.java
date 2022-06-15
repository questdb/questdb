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

import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableReaderMetadata;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.cairo.sql.OperationFuture;
import io.questdb.cairo.sql.ReaderOutOfDateException;
import io.questdb.cutlass.line.tcp.load.LineData;
import io.questdb.cutlass.line.tcp.load.TableData;
import io.questdb.griffin.*;
import io.questdb.griffin.engine.ops.AbstractOperation;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.SCSequence;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.std.Chars;
import io.questdb.std.Os;
import io.questdb.std.QuietClosable;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.Timestamps;
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
    private final ConcurrentLinkedQueue<TableSql> updatesSql = new ConcurrentLinkedQueue<>();
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

    private boolean checkTableAllRowsReceived(TableData table) {
        CharSequence tableName = table.getName();
        try (TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, tableName)) {
            getLog().info().$("table.getName(): ").$(table.getName()).$(", tableName: ").$(tableName).$(", table.size(): ").$(table.size()).$(", reader.size(): ").$(reader.size()).$();
            return table.size() == reader.size();

        }
    }

    private void executeUpdate(SqlCompiler compiler, SqlExecutionContext sqlExecutionContext, String sql, SCSequence waitSequence) throws SqlException {
        while (true) {
            try {
                CompiledQuery cc = compiler.compile(sql, sqlExecutionContext);

                LOG.info().$(sql).$();
                try (
                        AbstractOperation op = cc.getOperation();
                        OperationFuture fut = cc.getDispatcher().execute(op, sqlExecutionContext, waitSequence)
                ) {
                    if (fut.await(30 * Timestamps.SECOND_MICROS) != QUERY_COMPLETE) {
                        throw SqlException.$(0, "update query timeout");
                    }
                }
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
    protected void startThread(int threadId, Socket socket, SOCountDownLatch threadPushFinished) {
        super.startThread(threadId, socket, threadPushFinished);
        while (numOfUpdateThreads-- > 0) {
            startUpdateThread(numOfUpdateThreads, updatesDone);
        }
    }

    @Override
    protected void waitDone() {
        updatesDone.await();

        SqlCompiler compiler = compilers[0];
        SqlExecutionContext executionContext = executionContexts[0];
        HashSet<CharSequence> doneTables = new HashSet<>();

        // Repeat all updates when all lines are guaranteed to be landed in the tables
        for (TableSql tableSql : updatesSql) {
            try {
                if (!doneTables.contains(tableSql.tableName)) {
                    final TableData table = tables.get(tableSql.tableName);
                    do {
                        table.await();
                    } while (!checkTableAllRowsReceived(table));
                    doneTables.add(tableSql.tableName);
                }

                executeUpdate(compiler, executionContext, tableSql.sql, null);
            } catch (SqlException e) {
                LOG.error().$("update failed").$((Throwable) e).$();
            }
        }
    }

    private List<ColumnNameType> getMetaData(Hashtable<CharSequence, ArrayList<ColumnNameType>> readerColumns, CharSequence tableName) {
        if (readerColumns.contains(tableName)) {
            return readerColumns.get(tableName);
        }
        try (TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, tableName)) {
            TableReaderMetadata metadata = reader.getMetadata();
            ArrayList<ColumnNameType> columns = new ArrayList<>();
            for (int i = metadata.getColumnCount() - 1; i > -1L; i--) {
                if (i != metadata.getTimestampIndex()) {
                    columns.add(new ColumnNameType(metadata.getColumnName(i), metadata.getColumnType(i)));
                }
            }
            readerColumns.put(tableName, columns);
            return columns;
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
                Hashtable<CharSequence, ArrayList<ColumnNameType>> readers = new Hashtable<>();
                SCSequence waitSequence = new SCSequence();
                SqlCompiler compiler = compilers[threadId];
                SqlExecutionContext executionContext = executionContexts[threadId];
                while (tableNames.size() == 0) {
                    Os.pause();
                }

                for (int j = 0; j < numOfUpdates; j++) {
                    final CharSequence tableName = pickCreatedTableName(rnd);
                    List<ColumnNameType> metadata = getMetaData(readers, tableName);
                    final TableData table = tables.get(tableName);
                    LineData line = table.getRandomValidLine(rnd);

                    Collections.shuffle(metadata);
                    sql = line.generateRandomUpdate(tableName, metadata, rnd);
                    executeUpdate(compiler, executionContext, sql, waitSequence);
                    this.updatesSql.add(new TableSql(tableName, sql));
                }
            } catch (Exception e) {
                Assert.fail("Data sending failed [e=" + e + ", sql=" + sql + "]");
                throw new RuntimeException(e);
            } finally {
                updatesDone.countDown();
            }
        }).start();
    }

    private static class TableSql {
        CharSequence tableName;
        String sql;

        TableSql(CharSequence tableName, String sql) {
            this.tableName = tableName;
            this.sql = sql;
        }
    }
}
