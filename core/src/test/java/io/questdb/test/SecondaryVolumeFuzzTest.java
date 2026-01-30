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
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.log.LogFactory;
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static io.questdb.test.tools.TestUtils.createSqlExecutionCtx;

/**
 * Fuzz tests for tables in secondary volumes.
 * Tests concurrent create, drop, rename, and query operations.
 */
public class SecondaryVolumeFuzzTest extends AbstractBootstrapTest {
    private static final String VOLUME_ALIAS = "SECONDARY_VOLUME";
    private static final int PG_PORT_DELTA = 20;
    private static final int PG_PORT = AbstractBootstrapTest.PG_PORT + PG_PORT_DELTA;
    private static String secondaryVolume;

    @BeforeClass
    public static void setUpStatic() throws Exception {
        AbstractBootstrapTest.setUpStatic();
        secondaryVolume = AbstractBootstrapTest.temp.newFolder("secondary", "volume").getAbsolutePath();
    }

    @AfterClass
    public static void tearDownStatic() {
        Assert.assertTrue(Files.rmdir(auxPath.of(secondaryVolume), true));
        AbstractBootstrapTest.tearDownStatic();
    }

    @Override
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration(
                HTTP_PORT + PG_PORT_DELTA,
                HTTP_MIN_PORT + PG_PORT_DELTA,
                PG_PORT,
                ILP_PORT + PG_PORT_DELTA,
                root,
                PropertyKey.CAIRO_WAL_SUPPORTED.getPropertyPath() + "=true",
                PropertyKey.CAIRO_VOLUMES.getPropertyPath() + '=' + VOLUME_ALIAS + "->" + secondaryVolume)
        );
    }

    @Test
    public void testConcurrentCreateDropInVolume() throws Exception {
        Assume.assumeFalse(Os.isWindows());
        assertMemoryLeak(() -> {
            try (ServerMain qdb = new ServerMain(getServerMainArgs())) {
                qdb.start();
                CairoEngine engine = qdb.getEngine();

                int iterations = 50;
                int threadCount = 4;
                AtomicReference<Throwable> error = new AtomicReference<>();
                CyclicBarrier barrier = new CyclicBarrier(threadCount);
                AtomicInteger successfulOps = new AtomicInteger();

                ObjList<Thread> threads = new ObjList<>();
                for (int t = 0; t < threadCount; t++) {
                    final int threadId = t;
                    threads.add(new Thread(() -> {
                        try (SqlExecutionContext ctx = createSqlExecutionCtx(engine)) {
                            barrier.await();
                            Rnd rnd = new Rnd(threadId, threadId);
                            for (int i = 0; i < iterations && error.get() == null; i++) {
                                String tableName = "fuzz_table_" + threadId;
                                try {
                                    // Create table in volume
                                    engine.execute(
                                            "CREATE TABLE IF NOT EXISTS " + tableName +
                                                    " (x LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY" +
                                                    " IN VOLUME '" + VOLUME_ALIAS + "'",
                                            ctx
                                    );

                                    // Insert some data
                                    engine.execute(
                                            "INSERT INTO " + tableName +
                                                    " SELECT x, timestamp_sequence('2024-01-01', 1000000) ts" +
                                                    " FROM long_sequence(" + (rnd.nextInt(100) + 1) + ")",
                                            ctx
                                    );

                                    // Drop the table
                                    engine.execute("DROP TABLE IF EXISTS " + tableName, ctx);

                                    successfulOps.incrementAndGet();
                                } catch (SqlException | CairoException e) {
                                    // Acceptable race conditions
                                    if (!isAcceptableError(e)) {
                                        throw e;
                                    }
                                } catch (TableReferenceOutOfDateException e) {
                                    // Acceptable during concurrent operations
                                }
                            }
                        } catch (Throwable e) {
                            if (!isAcceptableError(e)) {
                                error.set(e);
                            }
                        } finally {
                            Path.clearThreadLocals();
                        }
                    }));
                    threads.getLast().start();
                }

                for (int i = 0; i < threads.size(); i++) {
                    threads.getQuick(i).join();
                }

                if (error.get() != null) {
                    throw new RuntimeException(error.get());
                }

                Assert.assertTrue("Should have completed some operations", successfulOps.get() > 0);
            }
        });
    }

    @Test
    public void testConcurrentRenameInVolume() throws Exception {
        Assume.assumeFalse(Os.isWindows());
        assertMemoryLeak(() -> {
            try (ServerMain qdb = new ServerMain(getServerMainArgs())) {
                qdb.start();
                CairoEngine engine = qdb.getEngine();

                // Create two tables in the volume
                try (SqlExecutionContext ctx = createSqlExecutionCtx(engine)) {
                    engine.execute(
                            "CREATE TABLE vol_t1 (x LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY" +
                                    " IN VOLUME '" + VOLUME_ALIAS + "'",
                            ctx
                    );
                    engine.execute(
                            "INSERT INTO vol_t1 SELECT x, timestamp_sequence('2024-01-01', 1000000) ts" +
                                    " FROM long_sequence(100)",
                            ctx
                    );

                    engine.execute(
                            "CREATE TABLE vol_t2 (x LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY" +
                                    " IN VOLUME '" + VOLUME_ALIAS + "'",
                            ctx
                    );
                    engine.execute(
                            "INSERT INTO vol_t2 SELECT x, timestamp_sequence('2024-01-01', 1000000) ts" +
                                    " FROM long_sequence(100)",
                            ctx
                    );
                }

                int queryThreads = 3;
                int renameIterations = 30;
                AtomicReference<Throwable> error = new AtomicReference<>();
                CyclicBarrier barrier = new CyclicBarrier(queryThreads + 1);
                AtomicBoolean done = new AtomicBoolean(false);

                ObjList<Thread> threads = new ObjList<>();

                // Query threads - continuously query the tables
                for (int t = 0; t < queryThreads; t++) {
                    threads.add(new Thread(() -> {
                        try (SqlExecutionContext ctx = createSqlExecutionCtx(engine)) {
                            barrier.await();
                            StringSink sink = new StringSink();
                            while (!done.get() && error.get() == null) {
                                try {
                                    // Try to query both tables
                                    TestUtils.printSql(engine, ctx, "SELECT count() FROM vol_t1", sink);
                                    TestUtils.printSql(engine, ctx, "SELECT count() FROM vol_t2", sink);
                                    TestUtils.printSql(engine, ctx,
                                            "SELECT * FROM vol_t1 JOIN vol_t2 ON vol_t1.ts = vol_t2.ts LIMIT 5",
                                            sink);
                                } catch (SqlException | CairoException e) {
                                    if (!isAcceptableError(e)) {
                                        throw e;
                                    }
                                } catch (TableReferenceOutOfDateException e) {
                                    // Expected during rename
                                }
                            }
                        } catch (Throwable e) {
                            if (!isAcceptableError(e)) {
                                error.set(e);
                            }
                        } finally {
                            Path.clearThreadLocals();
                        }
                    }));
                    threads.getLast().start();
                }

                // Rename thread - swap table names back and forth
                threads.add(new Thread(() -> {
                    try (
                            SqlCompiler compiler = engine.getSqlCompiler();
                            SqlExecutionContext ctx = createSqlExecutionCtx(engine)
                    ) {
                        barrier.await();
                        for (int i = 0; i < renameIterations && error.get() == null; i++) {
                            try {
                                // Swap vol_t1 and vol_t2 using a temp name
                                compiler.compile("RENAME TABLE vol_t1 TO vol_temp", ctx);
                                compiler.compile("RENAME TABLE vol_t2 TO vol_t1", ctx);
                                compiler.compile("RENAME TABLE vol_temp TO vol_t2", ctx);
                            } catch (SqlException | CairoException e) {
                                if (!isAcceptableError(e)) {
                                    throw e;
                                }
                            } catch (TableReferenceOutOfDateException e) {
                                // Expected
                            }
                        }
                    } catch (Throwable e) {
                        if (!isAcceptableError(e)) {
                            error.set(e);
                        }
                    } finally {
                        done.set(true);
                        Path.clearThreadLocals();
                    }
                }));
                threads.getLast().start();

                for (int i = 0; i < threads.size(); i++) {
                    threads.getQuick(i).join();
                }

                if (error.get() != null) {
                    throw new RuntimeException(error.get());
                }

                // Verify that tables still exist and are queryable
                // Note: after concurrent renames, names may have been swapped
                try (SqlExecutionContext ctx = createSqlExecutionCtx(engine)) {
                    StringSink sink = new StringSink();
                    int tablesFound = 0;

                    // Try to query each possible table name
                    for (String name : new String[]{"vol_t1", "vol_t2", "vol_temp"}) {
                        try {
                            TestUtils.printSql(engine, ctx, "SELECT count() FROM " + name, sink);
                            if (sink.toString().contains("100")) {
                                tablesFound++;
                            }
                            sink.clear();
                        } catch (SqlException | CairoException e) {
                            // Table may not exist with this name
                        }
                    }
                    Assert.assertTrue("Should find at least one table with 100 rows", tablesFound >= 1);

                    // Cleanup - use IF EXISTS since names may have been swapped
                    engine.execute("DROP TABLE IF EXISTS vol_t1", ctx);
                    engine.execute("DROP TABLE IF EXISTS vol_t2", ctx);
                    engine.execute("DROP TABLE IF EXISTS vol_temp", ctx);
                }
            }
        });
    }

    @Test
    public void testConcurrentQueryDuringDropRecreate() throws Exception {
        Assume.assumeFalse(Os.isWindows());
        assertMemoryLeak(() -> {
            try (ServerMain qdb = new ServerMain(getServerMainArgs())) {
                qdb.start();
                CairoEngine engine = qdb.getEngine();

                String tableName = "query_drop_table";

                // Create initial table
                try (SqlExecutionContext ctx = createSqlExecutionCtx(engine)) {
                    engine.execute(
                            "CREATE TABLE " + tableName +
                                    " (x LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY" +
                                    " IN VOLUME '" + VOLUME_ALIAS + "'",
                            ctx
                    );
                    engine.execute(
                            "INSERT INTO " + tableName +
                                    " SELECT x, timestamp_sequence('2024-01-01', 1000000) ts" +
                                    " FROM long_sequence(1000)",
                            ctx
                    );
                }

                int queryThreads = 4;
                int dropRecreateIterations = 20;
                AtomicReference<Throwable> error = new AtomicReference<>();
                CyclicBarrier barrier = new CyclicBarrier(queryThreads + 1);
                AtomicBoolean done = new AtomicBoolean(false);
                AtomicInteger queryCount = new AtomicInteger();

                ObjList<Thread> threads = new ObjList<>();

                // Query threads
                for (int t = 0; t < queryThreads; t++) {
                    threads.add(new Thread(() -> {
                        try (SqlExecutionContext ctx = createSqlExecutionCtx(engine)) {
                            barrier.await();
                            StringSink sink = new StringSink();
                            while (!done.get() && error.get() == null) {
                                try {
                                    TestUtils.printSql(engine, ctx,
                                            "SELECT count(), sum(x) FROM " + tableName,
                                            sink);
                                    queryCount.incrementAndGet();
                                } catch (SqlException | CairoException e) {
                                    if (!isAcceptableError(e)) {
                                        throw e;
                                    }
                                } catch (TableReferenceOutOfDateException e) {
                                    // Expected
                                }
                            }
                        } catch (Throwable e) {
                            if (!isAcceptableError(e)) {
                                error.set(e);
                            }
                        } finally {
                            Path.clearThreadLocals();
                        }
                    }));
                    threads.getLast().start();
                }

                // Drop/recreate thread
                threads.add(new Thread(() -> {
                    try (SqlExecutionContext ctx = createSqlExecutionCtx(engine)) {
                        barrier.await();
                        Rnd rnd = new Rnd();
                        for (int i = 0; i < dropRecreateIterations && error.get() == null; i++) {
                            try {
                                // Drop
                                engine.execute("DROP TABLE IF EXISTS " + tableName, ctx);

                                // Small delay
                                Os.sleep(rnd.nextInt(5));

                                // Recreate
                                engine.execute(
                                        "CREATE TABLE " + tableName +
                                                " (x LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY" +
                                                " IN VOLUME '" + VOLUME_ALIAS + "'",
                                        ctx
                                );

                                // Insert data
                                engine.execute(
                                        "INSERT INTO " + tableName +
                                                " SELECT x, timestamp_sequence('2024-01-01', 1000000) ts" +
                                                " FROM long_sequence(" + (rnd.nextInt(500) + 100) + ")",
                                        ctx
                                );
                            } catch (SqlException | CairoException e) {
                                if (!isAcceptableError(e)) {
                                    throw e;
                                }
                            }
                        }
                    } catch (Throwable e) {
                        if (!isAcceptableError(e)) {
                            error.set(e);
                        }
                    } finally {
                        done.set(true);
                        Path.clearThreadLocals();
                    }
                }));
                threads.getLast().start();

                for (int i = 0; i < threads.size(); i++) {
                    threads.getQuick(i).join();
                }

                if (error.get() != null) {
                    throw new RuntimeException(error.get());
                }

                Assert.assertTrue("Should have executed some queries", queryCount.get() > 0);

                // Cleanup
                try (SqlExecutionContext ctx = createSqlExecutionCtx(engine)) {
                    engine.execute("DROP TABLE IF EXISTS " + tableName, ctx);
                }
            }
        });
    }

    @Test
    public void testMixedVolumeOperations() throws Exception {
        // Test operations on tables in both main volume and secondary volume concurrently
        Assume.assumeFalse(Os.isWindows());
        assertMemoryLeak(() -> {
            try (ServerMain qdb = new ServerMain(getServerMainArgs())) {
                qdb.start();
                CairoEngine engine = qdb.getEngine();

                int iterations = 30;
                int threadCount = 6;
                AtomicReference<Throwable> error = new AtomicReference<>();
                CyclicBarrier barrier = new CyclicBarrier(threadCount);

                ObjList<Thread> threads = new ObjList<>();

                // Threads operating on main volume tables
                for (int t = 0; t < threadCount / 2; t++) {
                    final int threadId = t;
                    threads.add(new Thread(() -> {
                        try (SqlExecutionContext ctx = createSqlExecutionCtx(engine)) {
                            barrier.await();
                            String tableName = "main_vol_" + threadId;
                            for (int i = 0; i < iterations && error.get() == null; i++) {
                                try {
                                    engine.execute(
                                            "CREATE TABLE IF NOT EXISTS " + tableName +
                                                    " (x LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY",
                                            ctx
                                    );
                                    engine.execute(
                                            "INSERT INTO " + tableName +
                                                    " SELECT x, timestamp_sequence('2024-01-01', 1000000) ts" +
                                                    " FROM long_sequence(50)",
                                            ctx
                                    );
                                    engine.execute("DROP TABLE IF EXISTS " + tableName, ctx);
                                } catch (SqlException | CairoException e) {
                                    if (!isAcceptableError(e)) {
                                        throw e;
                                    }
                                } catch (TableReferenceOutOfDateException e) {
                                    // Expected
                                }
                            }
                        } catch (Throwable e) {
                            if (!isAcceptableError(e)) {
                                error.set(e);
                            }
                        } finally {
                            Path.clearThreadLocals();
                        }
                    }));
                    threads.getLast().start();
                }

                // Threads operating on secondary volume tables
                for (int t = 0; t < threadCount / 2; t++) {
                    final int threadId = t;
                    threads.add(new Thread(() -> {
                        try (SqlExecutionContext ctx = createSqlExecutionCtx(engine)) {
                            barrier.await();
                            String tableName = "sec_vol_" + threadId;
                            for (int i = 0; i < iterations && error.get() == null; i++) {
                                try {
                                    engine.execute(
                                            "CREATE TABLE IF NOT EXISTS " + tableName +
                                                    " (x LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY" +
                                                    " IN VOLUME '" + VOLUME_ALIAS + "'",
                                            ctx
                                    );
                                    engine.execute(
                                            "INSERT INTO " + tableName +
                                                    " SELECT x, timestamp_sequence('2024-01-01', 1000000) ts" +
                                                    " FROM long_sequence(50)",
                                            ctx
                                    );
                                    engine.execute("DROP TABLE IF EXISTS " + tableName, ctx);
                                } catch (SqlException | CairoException e) {
                                    if (!isAcceptableError(e)) {
                                        throw e;
                                    }
                                } catch (TableReferenceOutOfDateException e) {
                                    // Expected
                                }
                            }
                        } catch (Throwable e) {
                            if (!isAcceptableError(e)) {
                                error.set(e);
                            }
                        } finally {
                            Path.clearThreadLocals();
                        }
                    }));
                    threads.getLast().start();
                }

                for (int i = 0; i < threads.size(); i++) {
                    threads.getQuick(i).join();
                }

                if (error.get() != null) {
                    throw new RuntimeException(error.get());
                }
            }
        });
    }

    @Test
    public void testRapidRenameSequence() throws Exception {
        // Rapidly rename a table many times and verify data integrity
        Assume.assumeFalse(Os.isWindows());
        assertMemoryLeak(() -> {
            try (ServerMain qdb = new ServerMain(getServerMainArgs())) {
                qdb.start();
                CairoEngine engine = qdb.getEngine();

                try (SqlExecutionContext ctx = createSqlExecutionCtx(engine)) {
                    // Create table with known data
                    engine.execute(
                            "CREATE TABLE rename_test (x LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY" +
                                    " IN VOLUME '" + VOLUME_ALIAS + "'",
                            ctx
                    );
                    engine.execute(
                            "INSERT INTO rename_test SELECT x, timestamp_sequence('2024-01-01', 1000000) ts" +
                                    " FROM long_sequence(1000)",
                            ctx
                    );

                    String currentName = "rename_test";

                    // Rapidly rename 50 times
                    for (int i = 0; i < 50; i++) {
                        String newName = "rename_test_" + i;
                        engine.execute("RENAME TABLE '" + currentName + "' TO '" + newName + "'", ctx);
                        currentName = newName;

                        // Verify data is still accessible
                        StringSink sink = new StringSink();
                        TestUtils.printSql(engine, ctx, "SELECT count() FROM " + currentName, sink);
                        Assert.assertTrue("Data should be preserved after rename " + i,
                                sink.toString().contains("1000"));
                    }

                    // Verify symlink points to correctly named directory
                    Assert.assertTrue("Volume directory should exist with final name",
                            Files.exists(auxPath.of(secondaryVolume).concat(currentName).$()));

                    // Cleanup
                    engine.execute("DROP TABLE " + currentName, ctx);

                    // Verify cleanup
                    Assert.assertFalse("Volume directory should be removed",
                            Files.exists(auxPath.of(secondaryVolume).concat(currentName).$()));
                }
            }
        });
    }

    private static boolean isAcceptableError(Throwable e) {
        CharSequence msg;
        if (e instanceof SqlException) {
            msg = ((SqlException) e).getFlyweightMessage();
        } else if (e instanceof CairoException) {
            // This includes EntryUnavailableException which is a subclass
            msg = ((CairoException) e).getFlyweightMessage();
        } else {
            return false;
        }
        return Chars.contains(msg, "table does not exist") ||
                Chars.contains(msg, "table already exists") ||
                Chars.contains(msg, "Could not lock") ||
                Chars.contains(msg, "cannot rename") ||
                Chars.contains(msg, "Rename target exists") ||
                Chars.contains(msg, "table is locked") ||
                Chars.contains(msg, "table busy") ||
                Chars.contains(msg, "busy") ||
                Chars.contains(msg, "Entry") ||
                Chars.contains(msg, "Unavailable") ||
                Chars.contains(msg, "unknown format");
    }

    static {
        LogFactory.getLog(SecondaryVolumeFuzzTest.class);
    }
}
