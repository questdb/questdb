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

import io.questdb.Bootstrap;
import io.questdb.PropertyKey;
import io.questdb.ServerMain;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.wal.ApplyWal2TableJob;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.log.LogFactory;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.std.*;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.Comparator;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static io.questdb.test.griffin.TableBackupTest.executeCreateTableStmt;
import static io.questdb.test.griffin.TableBackupTest.executeInsertGeneratorStmt;
import static io.questdb.test.tools.TestUtils.*;

public class ServerMainBackupDatabaseTest extends AbstractBootstrapTest {

    private static final int N = 7;
    private static final int pgPortDelta = 19;
    private static final int pgPort = PG_PORT + pgPortDelta;
    private static final StringSink sink = new StringSink();
    private static String backupRoot;
    private final Rnd rnd = TestUtils.generateRandom(LOG);
    private boolean isWal;
    private int partitionBy;

    @BeforeClass
    public static void setUpStatic() throws Exception {
        AbstractBootstrapTest.setUpStatic();
        try {
            backupRoot = temp.newFolder("backups").getAbsolutePath();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Before
    public void setUp() {
        super.setUp();

        isWal = rnd.nextBoolean();
        this.partitionBy = rnd.nextInt(PartitionBy.WEEK + 1);
        if (this.partitionBy == PartitionBy.NONE) {
            this.partitionBy = PartitionBy.DAY;
        }

        TestUtils.unchecked(() -> createDummyConfiguration(
                HTTP_PORT + pgPortDelta,
                HTTP_MIN_PORT + pgPortDelta,
                pgPort,
                ILP_PORT + pgPortDelta,
                root,
                PropertyKey.CAIRO_WAL_SUPPORTED.getPropertyPath() + "=true",
                PropertyKey.CAIRO_SQL_BACKUP_ROOT.getPropertyPath() + '=' + backupRoot
        ));
    }

    @Test
    public void testServerMainBackupDatabase() throws Exception {
        assertMemoryLeak(() -> {
            // The intention is to simulate a busy server that executes a request to backup the database (point in time).
            // At least 30% of the rows 'generated' should make it into the backup 3 seconds after the test starts. The
            // backup is open by another server and all tables are expected to be in place, fully readable. Steps:
            // - create N tables concurrently with a create generator statement, for all supported data types
            // - create N concurrent table writers executing insert generator statements. They start once the ^ ^ have completed
            // - create a database backup thread that will perform the backup 3 seconds after all threads have started
            // - keep track of the table tokens that made it into the backup and the number of overall rows expected in the backup
            // - the first error breaks the test

            AtomicReference<ObjList<TableToken>> tableTokens = new AtomicReference<>(new ObjList<>());
            AtomicLong expectedRowsOnBackUpStarted;

            try (
                    ServerMain qdb = new ServerMain(getServerMainArgs());
                    SqlCompiler defaultCompiler = qdb.getEngine().getSqlCompiler();
                    SqlExecutionContext defaultContext = createSqlExecutionCtx(qdb.getEngine())
            ) {
                qdb.start();

                CyclicBarrier createsBarrier = new CyclicBarrier(N);
                SOCountDownLatch createsCompleted = new SOCountDownLatch(N);
                SOCountDownLatch writersCompleted = new SOCountDownLatch(N);
                SOCountDownLatch backupCompleted = new SOCountDownLatch(1);
                AtomicBoolean endWriters = new AtomicBoolean(false);
                AtomicReference<ObjList<Throwable>> errors = new AtomicReference<>(new ObjList<>());

                CairoEngine engine = qdb.getEngine();
                ObjList<SqlExecutionContext> contexts = new ObjList<>();
                AtomicLong expectedTotalRows = new AtomicLong();

                // create/populate tables concurrently
                for (int t = 0; t < N; t++) {
                    contexts.add(createSqlExecutionCtx(engine));
                    startTableCreator(partitionBy, isWal, engine, contexts.get(t), tableTokens, createsBarrier, expectedTotalRows, createsCompleted, errors);
                }
                // insert into tables concurrently
                for (int t = 0; t < N; t++) {
                    contexts.add(createSqlExecutionCtx(engine));
                    startTableWriter(t, engine, isWal, contexts.get(t), tableTokens, expectedTotalRows, createsCompleted, writersCompleted, endWriters, errors);
                }

                // backup database concurrently 3 seconds from now
                expectedRowsOnBackUpStarted = startBackupDatabase(defaultCompiler, defaultContext, expectedTotalRows, createsCompleted, backupCompleted, errors);

                // wait for the backup to complete, end the writers
                Assert.assertTrue(backupCompleted.await(TimeUnit.SECONDS.toNanos(60L)));
                endWriters.set(true);
                Assert.assertTrue(writersCompleted.await(TimeUnit.SECONDS.toNanos(10L)));
                for (int i = 0, n = contexts.size(); i < n; i++) {
                    Misc.free(contexts.get(i));
                }
                contexts.clear();

                // drop tables
                ObjList<TableToken> tokens = tableTokens.get();
                for (int i = 0, n = tokens.size(); i < n; i++) {
                    dropTable(defaultCompiler, defaultContext, tokens.getQuick(i));
                }

                // fail on first error found
                String error = null;
                if (errors.get().size() > 0) {
                    error = errors.get().get(0).getMessage();
                }
                errors.get().clear();
                if (error != null) {
                    Assert.fail(error);
                }
            }

            // start the server on the last backup
            String newRoot = getLatestBackupDbRoot();
            try (
                    ServerMain qdb = new ServerMain("-d", newRoot, Bootstrap.SWITCH_USE_DEFAULT_LOG_FACTORY_CONFIGURATION);
                    SqlExecutionContext defaultContext = createSqlExecutionCtx(qdb.getEngine())
            ) {
                qdb.start();
                long totalRows = 0L;
                for (int i = 0, n = tableTokens.get().size(); i < n; i++) {
                    TableToken tableToken = tableTokens.get().get(i);
                    totalRows += assertTableExists(tableToken, partitionBy, isWal, qdb.getEngine(), defaultContext);
                    executeInsertGeneratorStmt(tableToken, 10, qdb.getEngine(), defaultContext);
                    drainWalQueue(qdb.getEngine());
                }
                long expected = expectedRowsOnBackUpStarted.get();
                Assert.assertTrue("expected min rows: " + expected + ", actual: " + totalRows, totalRows > expected);
            } finally {
                Assert.assertTrue(Files.rmdir(dbPath.of(newRoot), true));
            }
        });
    }

    private static long assertTableExists(
            TableToken tableToken,
            int partitionBy,
            boolean isWal,
            CairoEngine engine,
            SqlExecutionContext context
    ) throws Exception {
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            CompiledQuery cc = compiler.compile(
                    "SELECT table_name, designatedTimestamp, partitionBy, walEnabled, directoryName " +
                            "FROM tables() " +
                            "WHERE table_name='" + tableToken.getTableName() + '\'',
                    context
            );
            try (RecordCursorFactory factory = cc.getRecordCursorFactory(); RecordCursor cursor = factory.getCursor(context)) {
                RecordMetadata metadata = factory.getMetadata();
                CursorPrinter.println(cursor, metadata, sink, false, false);
                String expected = tableToken.getTableName() + "\ttimestamp2\t" + PartitionBy.toString(partitionBy) + '\t' + isWal + '\t' + tableToken.getDirName();
                TestUtils.assertContains(sink, expected);
            }

            cc = compiler.compile("SELECT * FROM '" + tableToken.getTableName() + "' WHERE bool = true LIMIT -100", context);
            try (RecordCursorFactory factory = cc.getRecordCursorFactory(); RecordCursor cursor = factory.getCursor(context)) {
                // being able to iterate is the test
                RecordMetadata metadata = factory.getMetadata();
                CursorPrinter.println(cursor, metadata, sink, false, false);
            }
            cc = compiler.compile("SELECT count(*) n FROM '" + tableToken.getTableName() + '\'', context);
            try (RecordCursorFactory factory = cc.getRecordCursorFactory(); RecordCursor cursor = factory.getCursor(context)) {
                RecordMetadata metadata = factory.getMetadata();
                CursorPrinter.println(cursor, metadata, sink, false, false);
                sink.clear(sink.length() - 1);
                return Long.parseLong(sink.toString());
            }
        }
    }

    private static String getLatestBackupDbRoot() {
        ObjList<File> roots = new ObjList<>();
        for (File f : Objects.requireNonNull(new File(backupRoot).listFiles())) {
            if (f.isDirectory()) {
                if (!f.getName().equals("tmp")) {
                    roots.add(f);
                } else {
                    Assert.assertTrue(Files.rmdir(auxPath.of(f.getAbsolutePath()), true));
                }
            }
        }
        int len = roots.size();
        Assert.assertTrue(len > 0);
        roots.sort(Comparator.comparing(File::lastModified));
        String newRoot = roots.get(len - 1).getAbsolutePath();
        for (int i = 0; i < len - 1; i++) {
            Assert.assertTrue(Files.rmdir(auxPath.of(roots.get(i).getAbsolutePath()), true));
        }
        return newRoot;
    }

    private static AtomicLong startBackupDatabase(
            SqlCompiler compiler,
            SqlExecutionContext context,
            AtomicLong expectedTotalRows,
            SOCountDownLatch createsCompleted,
            SOCountDownLatch backupCompleted,
            AtomicReference<ObjList<Throwable>> errors
    ) {
        AtomicLong expectedRowsOnBackUpStarted = new AtomicLong();
        startThread(backupCompleted, errors, () -> {
            long deadline = System.currentTimeMillis() + 3000L;
            while (!createsCompleted.await(TimeUnit.SECONDS.toNanos(3L))) {
                Os.sleep(1L);
            }
            while (System.currentTimeMillis() < deadline) {
                Os.sleep(200L);
            }
            expectedRowsOnBackUpStarted.set(expectedTotalRows.get());
            Assert.assertTrue(expectedRowsOnBackUpStarted.get() > 0);
            compiler.compile("BACKUP DATABASE", context);
            return null;
        });
        return expectedRowsOnBackUpStarted;
    }

    private static void startTableCreator(
            int partitionBy,
            boolean isWal,
            CairoEngine engine,
            SqlExecutionContext context,
            AtomicReference<ObjList<TableToken>> tableTokens,
            CyclicBarrier creatorsBarrier,
            AtomicLong expectedTotalRows,
            SOCountDownLatch createsCompleted,
            AtomicReference<ObjList<Throwable>> errors
    ) {
        startThread(createsCompleted, errors, () -> {
            creatorsBarrier.await();
            int numRows = ThreadLocalRandom.current().nextInt(100, 2000);
            tableTokens.get().add(executeCreateTableStmt(UUID.randomUUID() + "_عظمة_", partitionBy, isWal, numRows, engine, context));
            expectedTotalRows.getAndAdd(numRows);
            return null;
        });
    }

    private static void startTableWriter(
            int tableId,
            CairoEngine engine,
            boolean isWal,
            SqlExecutionContext context,
            AtomicReference<ObjList<TableToken>> tableTokens,
            AtomicLong expectedTotalRows,
            SOCountDownLatch createsCompleted,
            SOCountDownLatch writersCompleted,
            AtomicBoolean endWriters,
            AtomicReference<ObjList<Throwable>> errors
    ) {
        startThread(writersCompleted, errors, () -> {
            TableToken tableToken = null;
            try (ApplyWal2TableJob job = new ApplyWal2TableJob(engine, 1, 1)) {
                while (!createsCompleted.await(TimeUnit.MILLISECONDS.toNanos(5L))) {
                    if (Thread.currentThread().isInterrupted() || endWriters.get()) {
                        return null;
                    }
                    if (tableId < tableTokens.get().size()) {
                        tableToken = tableTokens.get().get(tableId);
                        break;
                    }
                }
                Assert.assertNotNull(tableToken);
                while (!Thread.currentThread().isInterrupted() && !endWriters.get()) {
                    int numRows = ThreadLocalRandom.current().nextInt(1, 50);
                    executeInsertGeneratorStmt(tableToken, numRows, engine, context);
                    if (isWal) {
                        while (job.run(0)) {
                            LOG.info().$("WAL apply ran").I$();
                        }
                    }
                    expectedTotalRows.getAndAdd(numRows);
                    Os.sleep(1L);
                }
            }

            return null;
        });
    }

    private static void startThread(SOCountDownLatch completedLatch, AtomicReference<ObjList<Throwable>> errors, Callable<Void> code) {
        new Thread(() -> {
            try {
                code.call();
            } catch (Throwable err) {
                errors.get().add(err);
            } finally {
                Path.clearThreadLocals();
                completedLatch.countDown();
            }
        }).start();
    }

    static {
        // log is needed to greedily allocate logger infra and
        // exclude it from leak detector
        LogFactory.getLog(ServerMainBackupDatabaseTest.class);
    }
}
