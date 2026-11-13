/*+*****************************************************************************
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

package io.questdb.test.cairo.view;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.pool.PoolListener;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.OperationFuture;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.view.ViewStateStore;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.ops.OperationDispatcher;
import io.questdb.mp.SCSequence;
import io.questdb.std.Chars;
import io.questdb.std.FilesFacade;
import io.questdb.std.Misc;
import io.questdb.std.Os;
import io.questdb.std.str.Path;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.LogCapture;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.regex.Pattern;

/**
 * A view reads a non-WAL table, and an ALTER TABLE changes the table's columns. The writer applies
 * the change outside the WAL, so no WAL apply job queues the recompile of the views that read the
 * table: the writer has to queue it. Without the recompile, the view keeps its old columns in the
 * catalogue, and stays valid after a change that breaks it (#7792).
 */
public class ViewOverNonWalTableTest extends AbstractViewTest {
    // 2025-06-19T12:00:00.000000Z
    private static final long TRUNCATE_MICROS = 1_750_334_400_000_000L;
    // a DROP, RENAME or TRUNCATE of t logs this line when it waits for a view compile run
    private static final String WAIT_LINE = ViewCompileParker.ddlWaitLine("t");
    private static final LogCapture capture = new LogCapture();
    private int addedColumnCount;

    @Before
    @Override
    public void setUp() {
        super.setUp();
        capture.start();
    }

    @After
    @Override
    public void tearDown() throws Exception {
        capture.stop();
        super.tearDown();
    }

    @Test
    public void testAddColumnFromBusyWriterUpdatesViewColumns() throws Exception {
        // Another thread holds the writer, so the statement queues the change for it. The view
        // keeps its columns until the writer applies the change.
        assertMemoryLeak(() -> {
            createTableAndView("t", "v");
            setCurrentMicros(ALTER_MICROS);
            try (
                    TableWriter writer = getWriter("t");
                    SqlCompiler compiler = engine.getSqlCompiler();
                    OperationFuture future = compiler.compile("ALTER TABLE t ADD COLUMN b INT", sqlExecutionContext)
                            .execute(new SCSequence())
            ) {
                Assert.assertEquals(OperationFuture.QUERY_NO_RESPONSE, future.getStatus());
                drainViewQueue();
                assertViewColumns("v", "a:INT");

                writer.tick(true);
                future.await();
            }
            drainViewQueue();
            assertViewColumns("v", "a:INT, b:INT");
            assertViewStatus("v", "valid", "", "2025-06-19T11:00:00.000000Z");
        });
    }

    @Test
    public void testAddColumnUpdatesViewColumns() throws Exception {
        assertMemoryLeak(() -> {
            createTableAndView("t", "v");
            createView("v_dep", "SELECT * FROM v");
            setCurrentMicros(ALTER_MICROS);

            execute("ALTER TABLE t ADD COLUMN b INT");
            execute("INSERT INTO t VALUES (3, 30)");
            drainViewQueue();

            assertViewColumns("v", "a:INT, b:INT");
            assertViewColumns("v_dep", "a:INT, b:INT");
            // SHOW COLUMNS reads the metadata cache, which only the view compiler updates
            assertQuery("SHOW COLUMNS FROM v")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tsymbolTableSize\tdesignated\tupsertKey\tindexType\tindexInclude
                            a\tINT\tfalse\t0\tfalse\t0\t0\tfalse\tfalse\t\t
                            b\tINT\tfalse\t0\tfalse\t0\t0\tfalse\tfalse\t\t
                            """);
            assertQuery("v_dep")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            a\tb
                            1\tnull
                            2\tnull
                            3\t30
                            """);
            assertViewStatus("v", "valid", "", "2025-06-19T11:00:00.000000Z");
            assertViewStatus("v_dep", "valid", "", "2025-06-19T11:00:00.000000Z");
        });
    }

    @Test
    public void testAddColumnWhenWriterFreesUpUpdatesViewColumns() throws Exception {
        // The writer is busy when the statement starts and free by the time it would queue the
        // change, so the statement applies the change in the writer itself.
        assertMemoryLeak(() -> {
            createTableAndView("t", "v");
            setCurrentMicros(ALTER_MICROS);
            final TableWriter[] holder = {getWriter("t")};
            OperationDispatcher.setAsyncEnqueueObserver(() -> holder[0] = Misc.free(holder[0]));
            try (
                    SqlCompiler compiler = engine.getSqlCompiler();
                    OperationFuture future = compiler.compile("ALTER TABLE t ADD COLUMN b INT", sqlExecutionContext)
                            .execute(new SCSequence())
            ) {
                Assert.assertNull(holder[0]);
                future.await();
            } finally {
                OperationDispatcher.setAsyncEnqueueObserver(null);
                Misc.free(holder[0]);
            }
            drainViewQueue();
            assertViewColumns("v", "a:INT, b:INT");
            assertViewStatus("v", "valid", "", "2025-06-19T11:00:00.000000Z");
        });
    }

    @Test
    public void testAlterColumnTypeUpdatesViewColumns() throws Exception {
        assertMemoryLeak(() -> {
            createTableAndView("t", "v");
            setCurrentMicros(ALTER_MICROS);

            execute("ALTER TABLE t ALTER COLUMN a TYPE LONG");
            drainViewQueue();

            assertViewColumns("v", "a:LONG");
            assertQuery("SELECT a, typeOf(a) FROM v")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            a\ttypeOf
                            1\tLONG
                            2\tLONG
                            """);
            assertViewStatus("v", "valid", "", "2025-06-19T11:00:00.000000Z");
        });
    }

    @Test
    public void testBusyRetryAfterTruncateSeesRowsInsertedBeforeItIsDue() throws Exception {
        // The compile that the ALTER queued meets the lock of TRUNCATE and leaves vp as it was. The
        // table is refilled before the busy retry is due, so the retry compiles vp over the rows and
        // leaves it valid with the new column, at the time of the ALTER.
        assertMemoryLeak(() -> {
            setCurrentMicros(CREATE_MICROS);
            createPivotTable();
            createView("vp", PIVOT_SQL);
            setCurrentMicros(ALTER_MICROS);
            execute("ALTER TABLE t ADD COLUMN c INT");
            setCurrentMicros(TRUNCATE_MICROS);
            executeWithViewCompileInsideLock("TRUNCATE TABLE t", PoolListener.EV_LOCK_SUCCESS, "t");
            final long dueMicros = TRUNCATE_MICROS + busyRetryTimeoutMicros();
            assertViewStatus("vp", "valid", "", "2025-06-19T10:00:00.000000Z");
            assertViewColumns("vp", "a:LONG, b:LONG");
            assertPendingCompileBusyRetries(pendingCompileBusyRetry("vp", ALTER_MICROS, dueMicros));

            execute("INSERT INTO t VALUES ('a', 3, 30)");
            setCurrentMicros(dueMicros);
            drainViewQueue();
            assertViewStatus("vp", "valid", "", "2025-06-19T11:00:00.000000Z");
            assertViewColumnsOfQuery("vp", PIVOT_SQL);
            assertPendingCompileBusyRetries("");
            assertSqlCursors(PIVOT_SQL, "vp");
        });
    }

    @Test
    public void testDdlAfterTableDroppedDuringViewCompileWait() throws Exception {
        // While DROP or RENAME of t waits for a view compile run, another session drops t. The DDL
        // reports that t does not exist, as a statement that starts after the drop does, and
        // DROP TABLE IF EXISTS returns quietly.
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_WRITER_ALTER_BUSY_WAIT_TIMEOUT, Long.MAX_VALUE);
            createTableWithRows("t2");
            createView("v2", "SELECT * FROM t2");
            assertDdlAfterTableGoneDuringViewCompileWait("DROP TABLE IF EXISTS t", null, false);
            assertDdlAfterTableGoneDuringViewCompileWait("DROP TABLE t", "table does not exist [table=t]", false);
            assertDdlAfterTableGoneDuringViewCompileWait("RENAME TABLE t TO u", "table does not exist [table=t]", false);
        });
    }

    @Test
    public void testDdlAfterTableNameReservedDuringViewCompileWait() throws Exception {
        // As above, but a CREATE TABLE in progress has reserved the name t by the time the DDL
        // checks the name again.
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_WRITER_ALTER_BUSY_WAIT_TIMEOUT, Long.MAX_VALUE);
            createTableWithRows("t2");
            createView("v2", "SELECT * FROM t2");
            assertDdlAfterTableGoneDuringViewCompileWait("DROP TABLE IF EXISTS t", null, true);
            assertDdlAfterTableGoneDuringViewCompileWait("DROP TABLE t", "table does not exist [table=t]", true);
            assertDdlAfterTableGoneDuringViewCompileWait("RENAME TABLE t TO u", "table does not exist [table=t]", true);
        });
    }

    @Test
    public void testDdlDoesNotActOnTableDroppedBeforeRetryLock() throws Exception {
        // Another session drops t after the DDL waited for a view compile run and found t under
        // its name, and before the DDL locks t again. The lock succeeds on the pools of the
        // dropped table, whose writer lock file sits next to the table directory, but the DDL
        // finds no table under the name while it holds the lock: it unlocks and fails as busy,
        // and leaves no directory or lock file behind.
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_WRITER_ALTER_BUSY_WAIT_TIMEOUT, Long.MAX_VALUE);
            createTableWithRows("t2");
            createView("v2", "SELECT * FROM t2");
            assertDdlDoesNotActOnTableChangedBeforeRetryLock("DROP TABLE t", "could not lock 't~' [reason='busyReader']", false);
            assertDdlDoesNotActOnTableChangedBeforeRetryLock("RENAME TABLE t TO u", "table busy [reason=busyReader]", false);
        });
    }

    @Test
    public void testDdlDoesNotActOnTableReplacedBeforeRetryLock() throws Exception {
        // Another session drops t and creates a new t after the DDL waited for a view compile run
        // and found t under its name, and before the DDL locks t again. The lock takes the new
        // table's pools, which share the directory name, but the DDL finds the new table under the
        // name while it holds the lock: it unlocks and fails as busy, and leaves the new table be.
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_WRITER_ALTER_BUSY_WAIT_TIMEOUT, Long.MAX_VALUE);
            createTableWithRows("t2");
            createView("v2", "SELECT * FROM t2");
            assertDdlDoesNotActOnTableChangedBeforeRetryLock("DROP TABLE t", "could not lock 't~' [reason='busyReader']", true);
            assertDdlDoesNotActOnTableChangedBeforeRetryLock("RENAME TABLE t TO u", "table busy [reason=busyReader]", true);
        });
    }

    @Test
    public void testDdlDoesNotActOnTableReplacedDuringViewCompileWait() throws Exception {
        // While DROP or RENAME of t waits for a view compile run, another session drops t and
        // creates a new t. The DDL must not act on the new table, which its statement never
        // resolved: it fails as busy, as it does today when a query holds the table.
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_WRITER_ALTER_BUSY_WAIT_TIMEOUT, Long.MAX_VALUE);
            createTableWithRows("t2");
            createView("v2", "SELECT * FROM t2");
            assertDdlDoesNotActOnTableReplacedDuringViewCompileWait("DROP TABLE t", "could not lock 't~' [reason='busyReader']");
            assertDdlDoesNotActOnTableReplacedDuringViewCompileWait("DROP TABLE IF EXISTS t", "could not lock 't~' [reason='busyReader']");
            assertDdlDoesNotActOnTableReplacedDuringViewCompileWait("RENAME TABLE t TO u", "table busy [reason=busyReader]");
        });
    }

    @Test
    public void testDdlDoesNotWaitAtBusyWriter() throws Exception {
        // A view compile run holds t's reader, but the DDL fails for another reason first: a
        // busy writer. The DDL fails at once, as it does today, and does not wait for the run.
        assertMemoryLeak(() -> assertDdlDoesNotWaitForViewCompile(() -> {
            try (TableWriter ignore = getWriter("t")) {
                assertDdlFails("DROP TABLE t", "could not lock 't~' [reason='test']");
                assertDdlFails("RENAME TABLE t TO u", "table busy [reason=test]");
            }
        }));
    }

    @Test
    public void testDdlDoesNotWaitAtCheckpoint() throws Exception {
        // CHECKPOINT CREATE calls sync(), which Windows does not have.
        Assume.assumeFalse(Os.isWindows());
        // A view compile run holds t's reader, but the DDL fails for another reason first: a
        // checkpoint in progress. The DDL fails at once, as it does today, and does not wait
        // for the run.
        assertMemoryLeak(() -> assertDdlDoesNotWaitForViewCompile(() -> {
            execute("CHECKPOINT CREATE");
            try {
                assertDdlFails("TRUNCATE TABLE t", "there is an active query against 't'. Try again.");
                assertDdlFails("DROP TABLE t", "could not lock 't~' [reason='checkpointInProgress']");
                assertDdlFails("RENAME TABLE t TO u", "table busy [reason=checkpointInProgress]");
            } finally {
                execute("CHECKPOINT RELEASE");
            }
        }));
    }

    @Test
    public void testDdlFailsAtOnceWhenQueryHoldsTable() throws Exception {
        // A query holds t's reader and no view compile runs. The DDL fails after one lock
        // attempt, as it does today, and does not wait.
        assertMemoryLeak(() -> {
            // the server default; a DDL that waited would log its wait before it fails
            node1.setProperty(PropertyKey.CAIRO_WRITER_ALTER_BUSY_WAIT_TIMEOUT, 500);
            createTableWithRows("t");
            createView("v", "SELECT * FROM t");
            final AtomicInteger lockAttempts = countTableMetadataLocks();
            try (
                    RecordCursorFactory factory = select("SELECT * FROM t");
                    RecordCursor ignore = factory.getCursor(sqlExecutionContext)
            ) {
                assertDdlFails("TRUNCATE TABLE t", "there is an active query against 't'. Try again.");
                assertDdlFails("DROP TABLE t", "could not lock 't~' [reason='busyReader']");
                Assert.assertEquals(1, lockAttempts.get());
                assertDdlFails("RENAME TABLE t TO u", "table busy [reason=busyReader]");
                Assert.assertEquals(2, lockAttempts.get());
            } finally {
                engine.setPoolListener(null);
            }
            capture.drain();
            capture.assertNotLogged(WAIT_LINE);

            execute("TRUNCATE TABLE t");
            execute("RENAME TABLE t TO u");
            execute("DROP TABLE u");
            assertTableListed("u", false);
        });
    }

    @Test
    public void testDdlFailsWhenViewCompileOutlastsBusyWaitTimeout() throws Exception {
        // A view compile run holds t's reader for longer than the busy wait timeout allows. The
        // DDL fails as it does today once the timeout passes, and at once when the timeout is 0.
        assertMemoryLeak(() -> {
            createTableAndViewsThenAddColumn();
            final ViewCompileParker run = ViewCompileParker.ofQueueDrain(engine, "t", PoolListener.SRC_READER);
            engine.setPoolListener(run);
            try {
                run.start();
                node1.setProperty(PropertyKey.CAIRO_WRITER_ALTER_BUSY_WAIT_TIMEOUT, 0);
                assertDdlFails("TRUNCATE TABLE t", "there is an active query against 't'. Try again.");
                assertDdlFails("DROP TABLE t", "could not lock 't~' [reason='busyReader']");
                assertDdlFails("RENAME TABLE t TO u", "table busy [reason=busyReader]");
                capture.drain();
                capture.assertNotLogged(WAIT_LINE);

                // the server default
                node1.setProperty(PropertyKey.CAIRO_WRITER_ALTER_BUSY_WAIT_TIMEOUT, 500);
                assertDdlFailsAfter(500, "TRUNCATE TABLE t", "there is an active query against 't'. Try again.");
                assertDdlFailsAfter(500, "DROP TABLE t", "could not lock 't~' [reason='busyReader']");
                assertDdlFailsAfter(500, "RENAME TABLE t TO u", "table busy [reason=busyReader]");
            } finally {
                run.join();
                engine.setPoolListener(null);
            }
            drainViewQueue();
            assertViewState("v");
            assertViewColumns("v", "a:INT, b:INT");

            execute("TRUNCATE TABLE t");
            execute("RENAME TABLE t TO u");
            execute("DROP TABLE u");
            assertTableListed("u", false);
        });
    }

    @Test
    public void testDdlRetriesOnceAfterViewCompileWhenQueryHoldsTable() throws Exception {
        // A query holds t's reader while a view compile run over another table runs. DROP cannot
        // tell the query from the run, so it waits for the run, tries the lock once more, and then
        // fails on the query as it does today.
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_WRITER_ALTER_BUSY_WAIT_TIMEOUT, Long.MAX_VALUE);
            createTableWithRows("t");
            createTableWithRows("t2");
            createView("v2", "SELECT * FROM t2");
            execute("ALTER TABLE t2 ADD COLUMN b INT");

            final AtomicInteger lockAttempts = countTableMetadataLocks();
            try (
                    RecordCursorFactory factory = select("SELECT * FROM t");
                    RecordCursor ignore = factory.getCursor(sqlExecutionContext)
            ) {
                ViewCompileParker.ofQueueDrain(engine, "t2", PoolListener.SRC_READER).executeWhileHolding(capture, "t", () -> {
                    assertDdlFails("DROP TABLE t", "could not lock 't~' [reason='busyReader']");
                    Assert.assertEquals(2, lockAttempts.get());
                }, null);
            } finally {
                engine.setPoolListener(null);
            }
            execute("DROP TABLE t");
            assertTableListed("t", false);
            drainViewQueue();
            assertViewState("v2");
            assertViewColumns("v2", "a:INT, b:INT");
        });
    }

    @Test
    public void testDropAllTablesWaitsForViewCompile() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_WRITER_ALTER_BUSY_WAIT_TIMEOUT, Long.MAX_VALUE);
            createTableAndViewsThenAddColumn();
            executeWhileViewCompileHolds("t", PoolListener.SRC_READER, "DROP ALL TABLES", null);
            assertTableListed("t", false);
            assertTableListed("v", false);
            assertTableListed("v_dep", false);
            Assert.assertFalse(isPresentInDbRoot("t~"));
        });
    }

    @Test
    public void testDropColumnInvalidatesView() throws Exception {
        assertMemoryLeak(() -> {
            setCurrentMicros(CREATE_MICROS);
            execute("CREATE TABLE t (a INT, c INT)");
            execute("INSERT INTO t VALUES (1, 10)");
            createView("v", "SELECT a, c FROM t");
            setCurrentMicros(ALTER_MICROS);

            execute("ALTER TABLE t DROP COLUMN c");
            drainViewQueue();

            assertViewState("v", "Invalid column: c");
            assertViewStatus("v", "invalid", "Invalid column: c", "2025-06-19T11:00:00.000000Z");

            // the column is back, and so is the view
            execute("ALTER TABLE t ADD COLUMN c INT");
            execute("INSERT INTO t VALUES (2, 20)");
            drainViewQueue();

            assertViewState("v");
            assertViewColumns("v", "a:INT, c:INT");
            assertQuery("v")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            a\tc
                            1\tnull
                            2\t20
                            """);
        });
    }

    @Test
    public void testDropRetriesWhenViewCompileStartsAndEndsInsideLockAttempt() throws Exception {
        // A view compile run starts after the DDL read the compile activity, borrows t's reader,
        // and ends before the DDL reads the activity again. No run is running at either read, but
        // a run started in between, so the DDL tries the lock again and succeeds.
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_WRITER_ALTER_BUSY_WAIT_TIMEOUT, Long.MAX_VALUE);
            createTableAndViewsThenAddColumn();
            assertDdlRetriesWhenViewCompileStartsAndEndsInsideLockAttempt("DROP TABLE t");
            assertTableListed("t", false);

            createTableWithRows("t");
            drainViewQueue();
            execute("ALTER TABLE t ADD COLUMN b INT");
            assertDdlRetriesWhenViewCompileStartsAndEndsInsideLockAttempt("RENAME TABLE t TO u");
            assertTableListed("t", false);
            assertQuery("u")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            a\tb
                            1\tnull
                            2\tnull
                            """);
        });
    }

    @Test
    public void testDropRightAfterAlterWaitsForViewCompile() throws Exception {
        // The ALTER queues the recompile of the views over t, and the run holds t's reader when
        // DROP locks t. DROP waits for the run instead of failing as busy.
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_WRITER_ALTER_BUSY_WAIT_TIMEOUT, Long.MAX_VALUE);
            createTableAndViewsThenAddColumn();
            executeWhileViewCompileHolds("t", PoolListener.SRC_READER, "DROP TABLE t", null);
            assertTableListed("t", false);
            Assert.assertFalse(isPresentInDbRoot("t~"));
            drainViewQueue();
            assertViewState("v", "table does not exist [table=t]");
        });
    }

    @Test
    public void testDropTableEndsBusyRetryOfViewCompileThatMetItsLock() throws Exception {
        // A view compile meets the lock that DROP TABLE holds on the table's readers, leaves the view
        // as it was, and leaves a busy retry. The compile that DROP queues marks the view invalid. The
        // retry then fails because the table does not exist, which is no lock, so nothing stays
        // pending, and its older update timestamp keeps the state that DROP's compile set.
        assertMemoryLeak(() -> {
            createTableAndView("t", "v");
            setCurrentMicros(ALTER_MICROS);
            execute("ALTER TABLE t ADD COLUMN b INT");
            // DROP at 12:00
            setCurrentMicros(TRUNCATE_MICROS);
            executeWithViewCompileInsideLock("DROP TABLE t", PoolListener.EV_LOCK_SUCCESS, "t");
            assertTableListed("t", false);
            final long dueMicros = TRUNCATE_MICROS + busyRetryTimeoutMicros();
            assertViewStatus("v", "valid", "", "2025-06-19T10:00:00.000000Z");
            assertViewColumns("v", "a:INT");
            assertPendingCompileBusyRetries(pendingCompileBusyRetry("v", ALTER_MICROS, dueMicros));

            drainViewQueue();
            assertViewStatus("v", "invalid", "table does not exist [table=t]", "2025-06-19T12:00:00.000000Z");
            assertPendingCompileBusyRetries(pendingCompileBusyRetry("v", ALTER_MICROS, dueMicros));

            setCurrentMicros(dueMicros);
            drainViewQueue();
            assertViewStatus("v", "invalid", "table does not exist [table=t]", "2025-06-19T12:00:00.000000Z");
            assertPendingCompileBusyRetries("");
        });
    }

    @Test
    public void testDropWaitsAgainWhenAnotherViewCompileTakesTheTable() throws Exception {
        // After DROP waited for one view compile run, another run takes t's reader before DROP
        // tries the lock again. DROP waits for that run too, and logs its wait only once.
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_WRITER_ALTER_BUSY_WAIT_TIMEOUT, Long.MAX_VALUE);
            createTableAndViewsThenAddColumn();
            final ViewCompileParker firstRun = ViewCompileParker.ofQueueDrain(engine, "t", PoolListener.SRC_READER);
            final ViewCompileParker secondRun = ViewCompileParker.ofQueueDrain(engine, "t", PoolListener.SRC_READER);
            final AtomicInteger lockAttempts = new AtomicInteger();
            final AtomicInteger unlocks = new AtomicInteger();
            final long ddlThread = Thread.currentThread().threadId();
            engine.setPoolListener((factoryType, thread, tableToken, event, segment, position) -> {
                firstRun.onEvent(factoryType, thread, tableToken, event, segment, position);
                secondRun.onEvent(factoryType, thread, tableToken, event, segment, position);
                if (isTableMetadataLockOfT(ddlThread, factoryType, thread, tableToken, event)
                        && lockAttempts.incrementAndGet() == 2) {
                    engine.enqueueCompileView(tableToken);
                    startRun(secondRun);
                } else if (isTableMetadataUnlockOfT(ddlThread, factoryType, thread, tableToken, event)) {
                    final int unlockCount = unlocks.incrementAndGet();
                    if (unlockCount == 1) {
                        firstRun.release();
                    } else if (unlockCount == 2) {
                        joinRun(secondRun);
                    }
                }
            });
            try {
                firstRun.start();
                execute("DROP TABLE t");
            } finally {
                firstRun.join();
                secondRun.join();
                engine.setPoolListener(null);
            }
            Assert.assertEquals(3, lockAttempts.get());
            capture.drain();
            capture.assertOnlyOnce(Pattern.quote(WAIT_LINE));
            assertTableListed("t", false);
        });
    }

    @Test
    public void testDropWaitsAtMostBusyWaitTimeoutOverAllWaits() throws Exception {
        // The busy wait timeout bounds all waits of one DDL together. The test clock stands still
        // while DROP waits for the first view compile run, and moves by the whole timeout before
        // DROP tries the lock again. DROP then gives up on a second run, which holds t, at once.
        assertMemoryLeak(() -> {
            final long timeoutMillis = 1_000;
            node1.setProperty(PropertyKey.CAIRO_WRITER_ALTER_BUSY_WAIT_TIMEOUT, timeoutMillis);
            createTableAndViewsThenAddColumn();
            setCurrentMicros(ALTER_MICROS);
            // runs on the DDL thread, which then reads the clock
            engine.setViewCompileRetryCallback(() -> currentMicros += timeoutMillis * 1_000);

            final ViewCompileParker firstRun = ViewCompileParker.ofQueueDrain(engine, "t", PoolListener.SRC_READER);
            final ViewCompileParker secondRun = ViewCompileParker.ofQueueDrain(engine, "t", PoolListener.SRC_READER);
            final AtomicInteger lockAttempts = new AtomicInteger();
            final AtomicInteger unlocks = new AtomicInteger();
            final long ddlThread = Thread.currentThread().threadId();
            engine.setPoolListener((factoryType, thread, tableToken, event, segment, position) -> {
                firstRun.onEvent(factoryType, thread, tableToken, event, segment, position);
                secondRun.onEvent(factoryType, thread, tableToken, event, segment, position);
                if (isTableMetadataLockOfT(ddlThread, factoryType, thread, tableToken, event)
                        && lockAttempts.incrementAndGet() == 2) {
                    engine.enqueueCompileView(tableToken);
                    startRun(secondRun);
                } else if (isTableMetadataUnlockOfT(ddlThread, factoryType, thread, tableToken, event)
                        && unlocks.incrementAndGet() == 1) {
                    firstRun.release();
                }
            });
            // a DROP that waited past the timeout would wait for the parked second run forever:
            // release that run after a while, so that such a DROP ends and fails the test
            final CountDownLatch isDropDone = new CountDownLatch(1);
            final Thread watchdog = new Thread(() -> {
                try {
                    if (!isDropDone.await(30, TimeUnit.SECONDS)) {
                        secondRun.release();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            watchdog.start();
            try {
                firstRun.start();
                assertDdlFails("DROP TABLE t", "could not lock 't~' [reason='busyReader']");
            } finally {
                isDropDone.countDown();
                watchdog.join();
                engine.setViewCompileRetryCallback(null);
                firstRun.join();
                secondRun.join();
                engine.setPoolListener(null);
            }
            Assert.assertEquals(2, lockAttempts.get());

            execute("DROP TABLE t");
            assertTableListed("t", false);
        });
    }

    @Test
    public void testDropWaitsForViewCompileHoldingTableMetadata() throws Exception {
        // A view over table_partitions('t') borrows t's pooled metadata, not its reader, when it
        // compiles. DROP and RENAME wait for that run too.
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_WRITER_ALTER_BUSY_WAIT_TIMEOUT, Long.MAX_VALUE);
            execute("CREATE TABLE t (a INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO t VALUES (1, '2024-01-01T00:00:00.000000Z')");
            Assert.assertFalse(engine.verifyTableName("t").isWal());
            createView("vp", "SELECT * FROM table_partitions('t')");

            execute("ALTER TABLE t ADD COLUMN b INT");
            executeWhileViewCompileHolds("t", PoolListener.SRC_TABLE_METADATA, "RENAME TABLE t TO u", null);
            assertTableListed("t", false);
            assertQuery("u")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            a\tts\tb
                            1\t2024-01-01T00:00:00.000000Z\tnull
                            """);

            execute("RENAME TABLE u TO t");
            drainViewQueue();
            execute("ALTER TABLE t ADD COLUMN c INT");
            executeWhileViewCompileHolds("t", PoolListener.SRC_TABLE_METADATA, "DROP TABLE t", null);
            assertTableListed("t", false);
            Assert.assertFalse(isPresentInDbRoot("t~"));
        });
    }

    @Test
    public void testFailedAlterKeepsView() throws Exception {
        // A concurrent change adds the column after the statement compiled, so the writer
        // rejects the statement. The failed statement must not recompile the view, and the
        // table and the view must keep working.
        assertMemoryLeak(() -> {
            createTableAndView("t", "v");
            setCurrentMicros(ALTER_MICROS);
            try (
                    TableWriter writer = getWriter("t");
                    SqlCompiler compiler = engine.getSqlCompiler()
            ) {
                final CompiledQuery compiledQuery = compiler.compile("ALTER TABLE t ADD COLUMN b INT", sqlExecutionContext);
                writer.addColumn("b", ColumnType.INT, AllowAllSecurityContext.INSTANCE);
                try (OperationFuture future = compiledQuery.execute(new SCSequence())) {
                    writer.tick(true);
                    try {
                        future.await();
                        Assert.fail("expected the writer to reject the duplicate column");
                    } catch (SqlException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "duplicate column [name=b]");
                    }
                }
            }
            drainViewQueue();
            assertViewStatus("v", "valid", "", "2025-06-19T10:00:00.000000Z");

            execute("ALTER TABLE t ADD COLUMN c INT");
            execute("INSERT INTO t VALUES (3, 30, 300)");
            drainViewQueue();
            assertViewColumns("v", "a:INT, b:INT, c:INT");
            assertQuery("v")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            a\tb\tc
                            1\tnull\tnull
                            2\tnull\tnull
                            3\t30\t300
                            """);
            assertViewStatus("v", "valid", "", "2025-06-19T11:00:00.000000Z");
        });
    }

    @Test
    public void testFailedDropOrRenameAttemptRetriesViewCompileThatMetTheLock() throws Exception {
        // A query holds t's reader in the second slot of the pool, and an idle reader sits in the
        // first. The lock attempt of DROP or RENAME closes the idle reader while it holds the lock of
        // t's readers, and a view compile that runs at that moment meets the lock. The attempt then
        // fails on the query. The compile leaves the view as it was, and its busy retry leaves the
        // view valid with the new column once the query ended.
        assertMemoryLeak(() -> {
            final String[][] ddls = {
                    {"DROP TABLE t", "could not lock 't~' [reason='busyReader']"},
                    {"RENAME TABLE t TO u", "table busy [reason=busyReader]"}
            };
            for (String[] ddl : ddls) {
                // the DDL gives up at once, or, at the server default, tries the lock once more
                for (long timeoutMillis : new long[]{0, 500}) {
                    node1.setProperty(PropertyKey.CAIRO_WRITER_ALTER_BUSY_WAIT_TIMEOUT, timeoutMillis);
                    createTableAndView("t", "v");
                    setCurrentMicros(ALTER_MICROS);
                    execute("ALTER TABLE t ADD COLUMN b INT");
                    final TableReader idleReader = engine.getReader("t");
                    final ReaderHoldingThread query = ReaderHoldingThread.start(engine, "t");
                    idleReader.close();
                    // the DDL at 12:00
                    setCurrentMicros(TRUNCATE_MICROS);
                    try {
                        executeWithViewCompileInsideLock(ddl[0], PoolListener.EV_LOCK_CLOSE, "t");
                        Assert.fail("expected to fail: " + ddl[0]);
                    } catch (SqlException | CairoException e) {
                        TestUtils.assertEquals(ddl[1], e.getFlyweightMessage());
                    } finally {
                        query.close();
                    }
                    final long dueMicros = TRUNCATE_MICROS + busyRetryTimeoutMicros();
                    assertViewStatus("v", "valid", "", "2025-06-19T10:00:00.000000Z");
                    assertViewColumns("v", "a:INT");
                    assertPendingCompileBusyRetries(pendingCompileBusyRetry("v", ALTER_MICROS, dueMicros));

                    setCurrentMicros(dueMicros);
                    drainViewQueue();
                    assertViewStatus("v", "valid", "", "2025-06-19T11:00:00.000000Z");
                    assertViewColumns("v", "a:INT, b:INT");
                    assertPendingCompileBusyRetries("");

                    // The failed attempt kept the first slot of t's reader pool. DROP TABLE on the
                    // same thread passes over the slot, and drops t's pool entry with the slot.
                    execute("DROP VIEW v");
                    execute("DROP TABLE t");
                    Assert.assertEquals(0, engine.getBusyReaderCount());
                    // the compiles that the drops queue must not compile the views of the next round
                    drainViewQueue();
                }
            }
        });
    }

    @Test
    public void testFailedTruncateRetriesViewsWhoseCompileMetTheLockAttempt() throws Exception {
        // A query holds t's reader in the second slot of the pool, and an idle reader sits in the
        // first. The lock attempt of TRUNCATE closes the idle reader while it holds the lock of t's
        // readers, and a view compile that runs at that moment meets the lock: it leaves the views
        // over t as they were, and retries their compile once the busy retry timeout passed. The
        // attempt then fails on the query, and so does TRUNCATE. The retry leaves the views valid
        // with the new column.
        assertMemoryLeak(() -> {
            // TRUNCATE gives up at once, or, at the server default, tries the lock once more
            for (long timeoutMillis : new long[]{0, 500}) {
                node1.setProperty(PropertyKey.CAIRO_WRITER_ALTER_BUSY_WAIT_TIMEOUT, timeoutMillis);
                createTableAndViewsThenAddColumnAtAlterTime();
                final TableReader idleReader = engine.getReader("t");
                final ReaderHoldingThread query = ReaderHoldingThread.start(engine, "t");
                idleReader.close();
                setCurrentMicros(TRUNCATE_MICROS);
                try {
                    executeWithViewCompileInsideLock("TRUNCATE TABLE t", PoolListener.EV_LOCK_CLOSE, "t");
                    Assert.fail("expected to fail: TRUNCATE TABLE t");
                } catch (SqlException e) {
                    TestUtils.assertEquals("there is an active query against 't'. Try again.", e.getFlyweightMessage());
                } finally {
                    query.close();
                }
                try {
                    assertQuery("t")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    a\tb
                                    1\tnull
                                    2\tnull
                                    """);
                    assertViewsKeptStateWithBusyRetries("a:INT", TRUNCATE_MICROS);
                    setCurrentMicros(TRUNCATE_MICROS + busyRetryTimeoutMicros());
                    drainViewQueue();
                    assertViewsValidAtAlterTime("a:INT, b:INT");
                } finally {
                    // The failed attempt kept the first slot of t's reader pool. TRUNCATE on the
                    // same thread passes over the slot, and its unlock drops t's pool entry with
                    // the slot.
                    execute("TRUNCATE TABLE t");
                }
                assertQuery("t")
                        .noLeakCheck()
                        .expectSize()
                        .returns("a\tb\n");
                Assert.assertEquals(0, engine.getBusyReaderCount());
                execute("DROP VIEW v_dep");
                execute("DROP VIEW v");
                execute("DROP TABLE t");
                // the compiles that the drops queue must not compile the views of the next round
                drainViewQueue();
            }
        });
    }

    @Test
    public void testNonStructuralAlterDoesNotRecompileView() throws Exception {
        assertMemoryLeak(() -> {
            setCurrentMicros(CREATE_MICROS);
            execute("CREATE TABLE t (s SYMBOL, a INT)");
            execute("INSERT INTO t VALUES ('x', 1)");
            createView("v", "SELECT * FROM t");
            setCurrentMicros(ALTER_MICROS);

            execute("ALTER TABLE t ALTER COLUMN s ADD INDEX");
            execute("ALTER TABLE t ALTER COLUMN s NOCACHE");
            drainViewQueue();
            assertViewStatus("v", "valid", "", "2025-06-19T10:00:00.000000Z");

            execute("ALTER TABLE t ALTER COLUMN s DROP INDEX");
            drainViewQueue();
            assertViewStatus("v", "valid", "", "2025-06-19T10:00:00.000000Z");
        });
    }

    @Test
    public void testPivotViewCompileInsideTruncateLockRetriesOverEmptiedTable() throws Exception {
        // The compile that the ALTER queued runs while TRUNCATE holds the lock of t's readers, and
        // leaves vp as it was. Its busy retry compiles vp over the emptied table once the busy retry
        // timeout passed, as COMPILE VIEW would then, and marks vp invalid with the PIVOT error, at
        // the time of the ALTER.
        assertMemoryLeak(() -> {
            setCurrentMicros(CREATE_MICROS);
            createPivotTable();
            createView("vp", PIVOT_SQL);
            setCurrentMicros(ALTER_MICROS);
            execute("ALTER TABLE t ADD COLUMN c INT");
            setCurrentMicros(TRUNCATE_MICROS);
            executeWithViewCompileInsideLock("TRUNCATE TABLE t", PoolListener.EV_LOCK_SUCCESS, "t");
            final long dueMicros = TRUNCATE_MICROS + busyRetryTimeoutMicros();
            assertViewStatus("vp", "valid", "", "2025-06-19T10:00:00.000000Z");
            assertViewColumns("vp", "a:LONG, b:LONG");
            assertPendingCompileBusyRetries(pendingCompileBusyRetry("vp", ALTER_MICROS, dueMicros));

            setCurrentMicros(dueMicros - 1);
            drainViewQueue();
            assertViewStatus("vp", "valid", "", "2025-06-19T10:00:00.000000Z");
            assertPendingCompileBusyRetries(pendingCompileBusyRetry("vp", ALTER_MICROS, dueMicros));

            setCurrentMicros(dueMicros);
            drainViewQueue();
            assertViewStatus("vp", "invalid", "PIVOT IN subquery returned empty result set", "2025-06-19T11:00:00.000000Z");
            assertPendingCompileBusyRetries("");

            execute("INSERT INTO t VALUES ('a', 3, 30)");
            // 2025-06-19T13:00:00.000000Z
            setCurrentMicros(1_750_338_000_000_000L);
            execute("COMPILE VIEW vp");
            drainViewQueue();
            assertViewStatus("vp", "valid", "", "2025-06-19T13:00:00.000000Z");
            assertViewColumnsOfQuery("vp", PIVOT_SQL);
            assertSqlCursors(PIVOT_SQL, "vp");
        });
    }

    @Test
    public void testRenameColumnInvalidatesView() throws Exception {
        assertMemoryLeak(() -> {
            setCurrentMicros(CREATE_MICROS);
            execute("CREATE TABLE t (a INT, c INT)");
            execute("INSERT INTO t VALUES (1, 10)");
            createView("v", "SELECT a, c FROM t");
            createView("v_all", "SELECT * FROM t");
            setCurrentMicros(ALTER_MICROS);

            execute("ALTER TABLE t RENAME COLUMN c TO d");
            drainViewQueue();

            assertViewState("v", "Invalid column: c");
            assertViewStatus("v", "invalid", "Invalid column: c", "2025-06-19T11:00:00.000000Z");
            assertViewColumns("v_all", "a:INT, d:INT");

            execute("ALTER TABLE t RENAME COLUMN d TO c");
            drainViewQueue();

            assertViewState("v");
            assertViewColumns("v_all", "a:INT, c:INT");
            assertQuery("v")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            a\tc
                            1\t10
                            """);
        });
    }

    @Test
    public void testRenameRightAfterAlterWaitsForViewCompile() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_WRITER_ALTER_BUSY_WAIT_TIMEOUT, Long.MAX_VALUE);
            createTableAndViewsThenAddColumn();
            executeWhileViewCompileHolds("t", PoolListener.SRC_READER, "RENAME TABLE t TO u", null);
            assertTableListed("t", false);
            assertQuery("u")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            a\tb
                            1\tnull
                            2\tnull
                            """);
            drainViewQueue();
            assertViewState("v", "table does not exist [table=t]");
        });
    }

    @Test
    public void testTruncateAfterCompileViewRetriesViewsWhoseCompileMetTheLock() throws Exception {
        // COMPILE VIEW, and CREATE VIEW, queue a compile of the view, which runs while TRUNCATE of
        // the table the view reads holds the lock of the table's readers. The compile leaves the
        // views as they were, and its busy retry leaves them valid once the busy retry timeout passed.
        assertMemoryLeak(() -> {
            setCurrentMicros(CREATE_MICROS);
            createTableWithRows("t");
            createView("v", "SELECT * FROM t");
            createView("v_dep", "SELECT * FROM v");
            setCurrentMicros(ALTER_MICROS);
            execute("COMPILE VIEW v");
            setCurrentMicros(TRUNCATE_MICROS);
            executeWithViewCompileInsideLock("TRUNCATE TABLE t", PoolListener.EV_LOCK_SUCCESS, "t");
            assertViewsKeptStateWithBusyRetries("a:INT", TRUNCATE_MICROS);
            final long dueMicros = TRUNCATE_MICROS + busyRetryTimeoutMicros();
            setCurrentMicros(dueMicros);
            drainViewQueue();
            assertViewsValidAtAlterTime("a:INT");

            // CREATE VIEW at 12:00:01 creates v2 valid, and the busy retry of its compile, which met
            // the lock of the next TRUNCATE, is not due at the drain
            execute("CREATE VIEW v2 AS (SELECT a FROM t)");
            executeWithViewCompileInsideLock("TRUNCATE TABLE t", PoolListener.EV_LOCK_SUCCESS, "t");
            drainViewQueue();
            assertViewStatus("v2", "valid", "", "2025-06-19T12:00:01.000000Z");
            assertViewColumns("v2", "a:INT");
            assertPendingCompileBusyRetries(pendingCompileBusyRetry("v2", dueMicros, dueMicros + busyRetryTimeoutMicros()));
        });
    }

    @Test
    public void testTruncateDuringUnrelatedViewCompileKeepsPivotViewValid() throws Exception {
        // A view compile run over the table u runs while TRUNCATE of t locks t's readers. TRUNCATE
        // queues no compile of the views over t: a compile over the emptied table would mark the
        // view whose PIVOT IN sub-query reads the rows of t invalid, and the INSERT that refills t
        // queues no compile.
        assertMemoryLeak(() -> {
            setCurrentMicros(CREATE_MICROS);
            createPivotTable();
            execute("CREATE TABLE u (a INT)");
            execute("INSERT INTO u VALUES (1)");
            createView("vp", PIVOT_SQL);
            createView("vu", "SELECT * FROM u");
            // queues a compile of the views over u
            setCurrentMicros(ALTER_MICROS);
            execute("ALTER TABLE u ADD COLUMN b INT");
            setCurrentMicros(TRUNCATE_MICROS);
            final ViewCompileParker run = ViewCompileParker.ofQueueDrain(engine, "u", PoolListener.SRC_READER);
            engine.setPoolListener(run);
            try {
                run.start();
                execute("TRUNCATE TABLE t");
            } finally {
                run.join();
                engine.setPoolListener(null);
            }
            assertViewStatus("vp", "valid", "", "2025-06-19T10:00:00.000000Z");
            // the run compiled the views over u
            assertViewStatus("vu", "valid", "", "2025-06-19T11:00:00.000000Z");
            assertViewColumns("vu", "a:INT, b:INT");

            drainViewQueue();
            assertViewStatus("vp", "valid", "", "2025-06-19T10:00:00.000000Z");

            execute("INSERT INTO t VALUES ('a', 3)");
            setCurrentMicros(TRUNCATE_MICROS + 2 * busyRetryTimeoutMicros());
            drainViewQueue();
            assertViewStatus("vp", "valid", "", "2025-06-19T10:00:00.000000Z");
            assertPendingCompileBusyRetries("");
            assertQuery("SELECT * FROM vp")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            a
                            3
                            """);
            assertSqlCursors(PIVOT_SQL, "vp");
        });
    }

    @Test
    public void testTruncateEmptyTableRetriesViewsWhoseCompileMetTheLock() throws Exception {
        // TRUNCATE of a table with no rows leaves the table's files be, and still locks the table's
        // readers. The views whose compile met the lock keep their state, and their busy retry
        // leaves them valid with the new column.
        assertMemoryLeak(() -> {
            setCurrentMicros(CREATE_MICROS);
            execute("CREATE TABLE t (a INT)");
            createViewsThenAddColumnAtAlterTime();
            setCurrentMicros(TRUNCATE_MICROS);
            executeWithViewCompileInsideLock("TRUNCATE TABLE t", PoolListener.EV_LOCK_SUCCESS, "t");
            assertViewsKeptStateWithBusyRetries("a:INT", TRUNCATE_MICROS);
            setCurrentMicros(TRUNCATE_MICROS + busyRetryTimeoutMicros());
            drainViewQueue();
            assertViewsValidAtAlterTime("a:INT, b:INT");
            assertQuery("v")
                    .noLeakCheck()
                    .expectSize()
                    .returns("a\tb\n");
        });
    }

    @Test
    public void testTruncateFailingOnFileRetriesViewsWhoseCompileMetTheLock() throws Exception {
        // A view compile meets the lock of TRUNCATE, which then fails to truncate a column file. The
        // views keep their state, the busy retry of their compile leaves them valid, and the table
        // works.
        final AtomicBoolean isTruncateFailing = new AtomicBoolean();
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public boolean truncate(long fd, long size) {
                if (isTruncateFailing.compareAndSet(true, false)) {
                    return false;
                }
                return super.truncate(fd, size);
            }
        };
        assertMemoryLeak(ff, () -> {
            createTableAndViewsThenAddColumnAtAlterTime();
            setCurrentMicros(TRUNCATE_MICROS);
            try {
                executeWithActionInsideLock(
                        "TRUNCATE TABLE t",
                        PoolListener.EV_LOCK_SUCCESS,
                        tableToken -> {
                            ViewCompileParker.runQueueDrain(engine);
                            isTruncateFailing.set(true);
                        },
                        "t"
                );
                Assert.fail("expected to fail: TRUNCATE TABLE t");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "Cannot truncate fd=");
            }
            Assert.assertFalse(isTruncateFailing.get());
            assertViewsKeptStateWithBusyRetries("a:INT", TRUNCATE_MICROS);
            setCurrentMicros(TRUNCATE_MICROS + busyRetryTimeoutMicros());
            drainViewQueue();
            assertViewsValidAtAlterTime("a:INT, b:INT");

            execute("TRUNCATE TABLE t");
            execute("INSERT INTO t VALUES (5, 50)");
            assertQuery("v")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            a\tb
                            5\t50
                            """);
        });
    }

    @Test
    public void testTruncateFailsWhenCheckpointStartsDuringViewCompileWait() throws Exception {
        // CHECKPOINT CREATE calls sync(), which Windows does not have.
        Assume.assumeFalse(Os.isWindows());
        // While TRUNCATE waits for a view compile run that holds t's reader, another session
        // starts a checkpoint. TRUNCATE checks for a checkpoint before each lock attempt, so once
        // the run ends it fails as it does when the checkpoint starts first, and leaves t's rows be.
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_WRITER_ALTER_BUSY_WAIT_TIMEOUT, Long.MAX_VALUE);
            createTableAndViewsThenAddColumn();
            try {
                executeWhileViewCompileHolds(
                        "t",
                        PoolListener.SRC_READER,
                        "TRUNCATE TABLE t",
                        () -> executeInOwnContext("CHECKPOINT CREATE")
                );
                Assert.fail("expected to fail: TRUNCATE TABLE t");
            } catch (SqlException e) {
                TestUtils.assertEquals("there is an active query against 't'. Try again.", e.getFlyweightMessage());
            } finally {
                execute("CHECKPOINT RELEASE");
            }
            capture.drain();
            capture.assertOnlyOnce(Pattern.quote(WAIT_LINE));
            assertQuery("t")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            a\tb
                            1\tnull
                            2\tnull
                            """);

            execute("TRUNCATE TABLE t");
            assertQuery("t")
                    .noLeakCheck()
                    .expectSize()
                    .returns("a\tb\n");
            drainViewQueue();
            assertViewState("v");
            assertViewState("v_dep");
            assertViewColumns("v", "a:INT, b:INT");
            assertViewColumns("v_dep", "a:INT, b:INT");
        });
    }

    @Test
    public void testTruncateKeepSymbolMapsRetriesViewsWhoseCompileMetTheLock() throws Exception {
        // TRUNCATE that keeps the symbol maps locks t's readers too. The views whose compile met the
        // lock keep their state, and their busy retry leaves them valid with the new column.
        assertMemoryLeak(() -> {
            setCurrentMicros(CREATE_MICROS);
            execute("CREATE TABLE t (a INT, s SYMBOL)");
            execute("INSERT INTO t VALUES (1, 'x'), (2, 'y')");
            createViewsThenAddColumnAtAlterTime();
            setCurrentMicros(TRUNCATE_MICROS);
            executeWithViewCompileInsideLock("TRUNCATE TABLE t KEEP SYMBOL MAPS", PoolListener.EV_LOCK_SUCCESS, "t");
            assertViewsKeptStateWithBusyRetries("a:INT, s:SYMBOL", TRUNCATE_MICROS);
            setCurrentMicros(TRUNCATE_MICROS + busyRetryTimeoutMicros());
            drainViewQueue();
            assertViewsValidAtAlterTime("a:INT, s:SYMBOL, b:INT");

            execute("INSERT INTO t VALUES (3, 'x', 30)");
            assertQuery("v")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            a\ts\tb
                            3\tx\t30
                            """);
        });
    }

    @Test
    public void testTruncateOfTwoTablesRetriesViewsWhoseCompileMetEachLock() throws Exception {
        // TRUNCATE locks the readers of one table after the other. While it holds each lock, a
        // compile of the views over that table runs and meets the lock. The views keep their state,
        // and each keeps one busy retry, with the newest of the update timestamps of the compiles
        // that met a lock. The retries leave all views valid with the new columns.
        assertMemoryLeak(() -> {
            setCurrentMicros(CREATE_MICROS);
            createTableWithRows("t3");
            createView("v3", "SELECT * FROM t3");
            createTableAndViewsThenAddColumnAtAlterTime();
            execute("ALTER TABLE t3 ADD COLUMN c INT");
            setCurrentMicros(TRUNCATE_MICROS);
            executeWithActionInsideLock(
                    "TRUNCATE TABLE t, t3",
                    PoolListener.EV_LOCK_SUCCESS,
                    tableToken -> {
                        // the compiles that the ALTERs queued run at the first lock, so queue a
                        // compile of the views over each table that TRUNCATE locks
                        engine.enqueueCompileView(tableToken);
                        ViewCompileParker.runQueueDrain(engine);
                    },
                    "t",
                    "t3"
            );
            assertViewStatus("v", "valid", "", "2025-06-19T10:00:00.000000Z");
            assertViewStatus("v_dep", "valid", "", "2025-06-19T10:00:00.000000Z");
            assertViewColumns("v", "a:INT");
            assertViewColumns("v_dep", "a:INT");
            // the compile that the ALTER of t3 queued ran at the lock of t, before TRUNCATE locked t3
            assertViewStatus("v3", "valid", "", "2025-06-19T11:00:00.000000Z");
            assertViewColumns("v3", "a:INT, c:INT");
            final long dueMicros = TRUNCATE_MICROS + busyRetryTimeoutMicros();
            assertPendingCompileBusyRetries(
                    pendingCompileBusyRetry("v", TRUNCATE_MICROS, dueMicros)
                            + pendingCompileBusyRetry("v3", TRUNCATE_MICROS, dueMicros)
                            + pendingCompileBusyRetry("v_dep", TRUNCATE_MICROS, dueMicros)
            );

            setCurrentMicros(dueMicros);
            drainViewQueue();
            assertViewStatus("v", "valid", "", "2025-06-19T12:00:00.000000Z");
            assertViewStatus("v_dep", "valid", "", "2025-06-19T12:00:00.000000Z");
            assertViewStatus("v3", "valid", "", "2025-06-19T12:00:00.000000Z");
            assertViewColumns("v", "a:INT, b:INT");
            assertViewColumns("v_dep", "a:INT, b:INT");
            assertViewColumns("v3", "a:INT, c:INT");
            assertPendingCompileBusyRetries("");
            assertQuery("t")
                    .noLeakCheck()
                    .expectSize()
                    .returns("a\tb\n");
            assertQuery("t3")
                    .noLeakCheck()
                    .expectSize()
                    .returns("a\tc\n");
        });
    }

    @Test
    public void testTruncateOfTwoTablesWaitsForViewCompile() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_WRITER_ALTER_BUSY_WAIT_TIMEOUT, Long.MAX_VALUE);
            createTableAndViewsThenAddColumn();
            createTableWithRows("t3");
            executeWhileViewCompileHolds("t", PoolListener.SRC_READER, "TRUNCATE TABLE t, t3", null);
            assertQuery("t")
                    .noLeakCheck()
                    .expectSize()
                    .returns("a\tb\n");
            assertQuery("t3")
                    .noLeakCheck()
                    .expectSize()
                    .returns("a\n");
        });
    }

    @Test
    public void testTruncatePartitionedTableRetriesViewsWhoseCompileMetTheLock() throws Exception {
        // TRUNCATE removes every partition of the table. The view over table_partitions() reads the
        // table's metadata, not its readers, so the compile that the ALTER queued compiles it before
        // TRUNCATE removes the partitions, and nothing compiles it again. The compiles of the other
        // views meet the lock, and their busy retry leaves them valid with the new column.
        assertMemoryLeak(() -> {
            setCurrentMicros(CREATE_MICROS);
            execute("CREATE TABLE t (a INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO t VALUES (1, '2024-01-01T00:00:00.000000Z'), (2, '2024-01-02T00:00:00.000000Z')");
            createView("vp", "SELECT name, numRows FROM table_partitions('t')");
            createViewsThenAddColumnAtAlterTime();
            setCurrentMicros(TRUNCATE_MICROS);
            executeWithViewCompileInsideLock("TRUNCATE TABLE t", PoolListener.EV_LOCK_SUCCESS, "t");
            assertViewStatus("vp", "valid", "", "2025-06-19T11:00:00.000000Z");
            assertViewsKeptStateWithBusyRetries("a:INT, ts:TIMESTAMP", TRUNCATE_MICROS);
            setCurrentMicros(TRUNCATE_MICROS + busyRetryTimeoutMicros());
            drainViewQueue();
            assertViewsValidAtAlterTime("a:INT, ts:TIMESTAMP, b:INT");
            assertViewStatus("vp", "valid", "", "2025-06-19T11:00:00.000000Z");
            assertQuery("v")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("a\tts\tb\n");
            assertQuery("vp")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("name\tnumRows\n");
        });
    }

    @Test
    public void testTruncatePartitionedTableRightAfterAlterWaitsForViewCompile() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_WRITER_ALTER_BUSY_WAIT_TIMEOUT, Long.MAX_VALUE);
            execute("CREATE TABLE t (a INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO t VALUES (1, '2024-01-01T00:00:00.000000Z'), (2, '2024-01-02T00:00:00.000000Z')");
            createViewsThenAddColumn();
            executeWhileViewCompileHolds("t", PoolListener.SRC_READER, "TRUNCATE TABLE t", null);
            assertQuery("t")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("a\tts\tb\n");
            drainViewQueue();
            assertViewState("v");
            assertViewState("v_dep");
            assertViewColumns("v", "a:INT, ts:TIMESTAMP, b:INT");
            assertViewColumns("v_dep", "a:INT, ts:TIMESTAMP, b:INT");
        });
    }

    @Test
    public void testTruncateRetriesJoinAndGroupByViewsWhoseCompileMetTheLock() throws Exception {
        // Views that join t, on either side, and a view that groups t compile while TRUNCATE holds
        // the lock of t's readers. The compiles leave the views as they were, and their busy retries
        // leave them valid with the new column.
        assertMemoryLeak(() -> {
            setCurrentMicros(CREATE_MICROS);
            createTableWithRows("t");
            createTableWithRows("t2");
            final String joinSql = "SELECT x.a, y.a a2 FROM t x JOIN t2 y ON x.a = y.a";
            // reads every column of t, so it gets the column that the ALTER adds
            final String joinAllSql = "SELECT y.*, x.a a2 FROM t2 x JOIN t y ON x.a = y.a";
            final String groupBySql = "SELECT a, count() c FROM t";
            createView("vj", joinSql);
            createView("vj2", joinAllSql);
            createView("vg", groupBySql);
            setCurrentMicros(ALTER_MICROS);
            execute("ALTER TABLE t ADD COLUMN b INT");
            setCurrentMicros(TRUNCATE_MICROS);
            executeWithViewCompileInsideLock("TRUNCATE TABLE t", PoolListener.EV_LOCK_SUCCESS, "t");
            assertViewStatus("vj", "valid", "", "2025-06-19T10:00:00.000000Z");
            assertViewStatus("vj2", "valid", "", "2025-06-19T10:00:00.000000Z");
            assertViewStatus("vg", "valid", "", "2025-06-19T10:00:00.000000Z");
            assertViewColumns("vj", "a:INT, a2:INT");
            assertViewColumns("vj2", "a:INT, a2:INT");
            assertViewColumns("vg", "a:INT, c:LONG");
            final long dueMicros = TRUNCATE_MICROS + busyRetryTimeoutMicros();
            assertPendingCompileBusyRetries(
                    pendingCompileBusyRetry("vg", ALTER_MICROS, dueMicros)
                            + pendingCompileBusyRetry("vj", ALTER_MICROS, dueMicros)
                            + pendingCompileBusyRetry("vj2", ALTER_MICROS, dueMicros)
            );

            setCurrentMicros(dueMicros);
            drainViewQueue();
            assertViewStatus("vj", "valid", "", "2025-06-19T11:00:00.000000Z");
            assertViewStatus("vj2", "valid", "", "2025-06-19T11:00:00.000000Z");
            assertViewStatus("vg", "valid", "", "2025-06-19T11:00:00.000000Z");
            assertViewColumns("vj", "a:INT, a2:INT");
            assertViewColumns("vj2", "a:INT, b:INT, a2:INT");
            assertViewColumns("vg", "a:INT, c:LONG");
            assertPendingCompileBusyRetries("");

            execute("INSERT INTO t VALUES (3, 30)");
            execute("INSERT INTO t2 VALUES (3)");
            assertQuery("vj")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            a\ta2
                            3\t3
                            """);
            assertQuery("vj2")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            a\tb\ta2
                            3\t30\t3
                            """);
            assertQuery("vg")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            a\tc
                            3\t1
                            """);
            assertSqlCursors(joinSql, "vj");
            assertSqlCursors(joinAllSql, "vj2");
            assertSqlCursors(groupBySql, "vg");
        });
    }

    @Test
    public void testTruncateRetriesOnceAfterViewCompileWhenQueryHoldsTable() throws Exception {
        // A query holds t's reader while a view compile run over another table runs. TRUNCATE
        // cannot tell the query from the run, so it waits for the run, tries the lock once more,
        // and then fails on the query as it does today, instead of trying until the timeout.
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_WRITER_ALTER_BUSY_WAIT_TIMEOUT, Long.MAX_VALUE);
            createTableWithRows("t");
            createTableWithRows("t2");
            createView("v2", "SELECT * FROM t2");
            execute("ALTER TABLE t2 ADD COLUMN b INT");

            final ReaderHoldingThread query = ReaderHoldingThread.start(engine, "t");
            try {
                ViewCompileParker.ofQueueDrain(engine, "t2", PoolListener.SRC_READER).executeWhileHolding(capture, "t", () -> {
                    // TRUNCATE reads the clock when it waits for view compile runs. One that kept trying
                    // the lock after the run ended would read it on every try, and never reach the
                    // timeout: end the query after many reads with no run running, so that such a
                    // TRUNCATE succeeds and fails the test.
                    final long ddlThread = Thread.currentThread().threadId();
                    final AtomicInteger clockReadsWithNoRun = new AtomicInteger();
                    testMicrosClock = () -> {
                        if (Thread.currentThread().threadId() == ddlThread
                                && !ViewStateStore.isCompileRunRunning(engine.getViewStateStore().getCompileRunActivity())
                                && clockReadsWithNoRun.incrementAndGet() == 1_000) {
                            closeQuery(query);
                        }
                        return defaultMicrosecondClock.getTicks();
                    };
                    assertDdlFails("TRUNCATE TABLE t", "there is an active query against 't'. Try again.");
                }, null);
            } finally {
                testMicrosClock = defaultMicrosecondClock;
                query.close();
            }
            capture.drain();
            capture.assertOnlyOnce(Pattern.quote(WAIT_LINE));
            assertQuery("t")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            a
                            1
                            2
                            """);

            execute("TRUNCATE TABLE t");
            assertQuery("t")
                    .noLeakCheck()
                    .expectSize()
                    .returns("a\n");
            drainViewQueue();
            assertViewState("v2");
            assertViewColumns("v2", "a:INT, b:INT");
        });
    }

    @Test
    public void testTruncateRetriesViewCompileThatMetLockInCodeGeneration() throws Exception {
        // The compile of a view that joins t with t2 borrows t's reader twice: the optimiser borrows
        // and returns it, then code generation borrows it again. TRUNCATE locks t's readers between
        // the two borrows, so the second one fails on the lock as the reader pool reports it, not as
        // the optimiser wraps it. The compile leaves the view as it was, and its busy retry leaves
        // the view valid with the new column.
        assertMemoryLeak(() -> {
            setCurrentMicros(CREATE_MICROS);
            createTableWithRows("t");
            createTableWithRows("t2");
            final String joinSql = "SELECT x.*, y.a a2 FROM t x JOIN t2 y ON x.a = y.a";
            createView("vj", joinSql);
            setCurrentMicros(ALTER_MICROS);
            execute("ALTER TABLE t ADD COLUMN b INT");
            setCurrentMicros(TRUNCATE_MICROS);

            // the run parks at its first borrow of t2
            final ViewCompileParker run = ViewCompileParker.ofQueueDrain(engine, "t2", PoolListener.SRC_READER);
            final StringBuffer runReaderEvents = new StringBuffer();
            final String[] runReaderEventsAtLock = new String[1];
            final long ddlThread = Thread.currentThread().threadId();
            engine.setPoolListener((factoryType, thread, tableToken, event, segment, position) -> {
                if (thread != ddlThread && factoryType == PoolListener.SRC_READER) {
                    recordReaderEvent(runReaderEvents, tableToken, event);
                }
                run.onEvent(factoryType, thread, tableToken, event, segment, position);
                if (thread == ddlThread
                        && factoryType == PoolListener.SRC_READER
                        && event == PoolListener.EV_LOCK_SUCCESS
                        && Chars.equals(tableToken.getTableName(), "t")
                        && runReaderEventsAtLock[0] == null) {
                    runReaderEventsAtLock[0] = runReaderEvents.toString();
                    // the run goes on to code generation, which borrows t while TRUNCATE holds the lock
                    joinRun(run);
                }
            });
            try {
                run.start();
                execute("TRUNCATE TABLE t");
            } finally {
                run.join();
                engine.setPoolListener(null);
            }
            Assert.assertEquals("t:borrow t:return t2:borrow ", runReaderEventsAtLock[0]);
            final long dueMicros = TRUNCATE_MICROS + busyRetryTimeoutMicros();
            assertViewStatus("vj", "valid", "", "2025-06-19T10:00:00.000000Z");
            assertViewColumns("vj", "a:INT, a2:INT");
            assertPendingCompileBusyRetries(pendingCompileBusyRetry("vj", ALTER_MICROS, dueMicros));

            setCurrentMicros(dueMicros);
            drainViewQueue();
            assertViewStatus("vj", "valid", "", "2025-06-19T11:00:00.000000Z");
            assertViewColumns("vj", "a:INT, b:INT, a2:INT");
            assertPendingCompileBusyRetries("");

            execute("INSERT INTO t VALUES (3, 30)");
            execute("INSERT INTO t2 VALUES (3)");
            assertSqlCursors(joinSql, "vj");
        });
    }

    @Test
    public void testTruncateRetriesViewsWhenViewCompileRunStartedBeforeIt() throws Exception {
        // A view compile run starts before TRUNCATE and holds t2's reader. TRUNCATE locks t's
        // readers, and the run goes on to compile the views over t, which meet the lock. The views
        // keep their state, and their busy retries leave them valid with the new column.
        assertMemoryLeak(() -> {
            setCurrentMicros(CREATE_MICROS);
            createTableWithRows("t");
            createTableWithRows("t2");
            createView("v", "SELECT * FROM t");
            createView("v_dep", "SELECT * FROM v");
            createView("v2", "SELECT * FROM t2");
            // the run compiles the views over t2 first, then the views over t
            execute("ALTER TABLE t2 ADD COLUMN c INT");
            setCurrentMicros(ALTER_MICROS);
            execute("ALTER TABLE t ADD COLUMN b INT");
            setCurrentMicros(TRUNCATE_MICROS);

            final ViewCompileParker run = ViewCompileParker.ofQueueDrain(engine, "t2", PoolListener.SRC_READER);
            final AtomicInteger lockCount = new AtomicInteger();
            final long ddlThread = Thread.currentThread().threadId();
            engine.setPoolListener((factoryType, thread, tableToken, event, segment, position) -> {
                run.onEvent(factoryType, thread, tableToken, event, segment, position);
                if (thread == ddlThread
                        && factoryType == PoolListener.SRC_READER
                        && event == PoolListener.EV_LOCK_SUCCESS
                        && Chars.equals(tableToken.getTableName(), "t")
                        && lockCount.incrementAndGet() == 1) {
                    // the run compiles the views over t while TRUNCATE holds the lock, and ends
                    joinRun(run);
                }
            });
            try {
                run.start();
                execute("TRUNCATE TABLE t");
            } finally {
                run.join();
                engine.setPoolListener(null);
            }
            Assert.assertEquals(1, lockCount.get());
            assertViewsKeptStateWithBusyRetries("a:INT", TRUNCATE_MICROS);
            setCurrentMicros(TRUNCATE_MICROS + busyRetryTimeoutMicros());
            drainViewQueue();
            assertViewsValidAtAlterTime("a:INT, b:INT");
            // the run compiled the views over t2 before TRUNCATE, and nothing compiled them since
            assertViewStatus("v2", "valid", "", "2025-06-19T10:00:00.000000Z");
            assertViewColumns("v2", "a:INT, c:INT");
        });
    }

    @Test
    public void testTruncateRightAfterAlterRetriesViewsWhoseCompileMetTheLock() throws Exception {
        // The ALTER queues the recompile of the views over t, which runs while TRUNCATE holds the
        // lock of t's readers: it cannot read t, and leaves the views valid with their old columns.
        // Its busy retry runs once the busy retry timeout passed, and leaves the views valid with
        // the new column, at the time of the ALTER.
        assertMemoryLeak(() -> {
            createTableAndViewsThenAddColumnAtAlterTime();
            setCurrentMicros(TRUNCATE_MICROS);
            executeWithViewCompileInsideLock("TRUNCATE TABLE t", PoolListener.EV_LOCK_SUCCESS, "t");
            assertViewsKeptStateWithBusyRetries("a:INT", TRUNCATE_MICROS);
            setCurrentMicros(TRUNCATE_MICROS + busyRetryTimeoutMicros());
            drainViewQueue();
            assertViewsValidAtAlterTime("a:INT, b:INT");
            // SHOW COLUMNS reads the metadata cache, which only the view compiler updates
            assertQuery("SHOW COLUMNS FROM v")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tsymbolTableSize\tdesignated\tupsertKey\tindexType\tindexInclude
                            a\tINT\tfalse\t0\tfalse\t0\t0\tfalse\tfalse\t\t
                            b\tINT\tfalse\t0\tfalse\t0\t0\tfalse\tfalse\t\t
                            """);
            assertQuery("v")
                    .noLeakCheck()
                    .expectSize()
                    .returns("a\tb\n");

            execute("INSERT INTO t VALUES (3, 30)");
            assertSqlCursors("SELECT * FROM t", "v");
            assertQuery("v_dep")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            a\tb
                            3\t30
                            """);
        });
    }

    @Test
    public void testTruncateRightAfterAlterWaitsForViewCompile() throws Exception {
        // The ALTER queues the recompile of the views over t, and the run holds t's reader when
        // TRUNCATE locks t's readers. TRUNCATE waits for the run instead of failing with
        // "there is an active query", and the run leaves the views valid with the new column.
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_WRITER_ALTER_BUSY_WAIT_TIMEOUT, Long.MAX_VALUE);
            createTableAndViewsThenAddColumn();
            executeWhileViewCompileHolds("t", PoolListener.SRC_READER, "TRUNCATE TABLE t", null);
            assertQuery("t")
                    .noLeakCheck()
                    .expectSize()
                    .returns("a\tb\n");
            drainViewQueue();
            assertViewState("v");
            assertViewState("v_dep");
            assertViewColumns("v", "a:INT, b:INT");
            assertViewColumns("v_dep", "a:INT, b:INT");
        });
    }

    @Test
    public void testTruncateWaitingForAlterViewCompileKeepsPivotViewValid() throws Exception {
        // The compile that the ALTER queued holds t's reader when TRUNCATE tries to lock t's
        // readers. TRUNCATE waits for the compile, which compiles vp over the full table, and then
        // queues no compile of its own: a compile over the emptied table would mark vp invalid.
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_WRITER_ALTER_BUSY_WAIT_TIMEOUT, Long.MAX_VALUE);
            setCurrentMicros(CREATE_MICROS);
            createPivotTable();
            createView("vp", PIVOT_SQL);
            setCurrentMicros(ALTER_MICROS);
            execute("ALTER TABLE t ADD COLUMN c INT");
            setCurrentMicros(TRUNCATE_MICROS);
            executeWhileViewCompileHolds("t", PoolListener.SRC_READER, "TRUNCATE TABLE t", null);
            assertQuery("t")
                    .noLeakCheck()
                    .expectSize()
                    .returns("k\tx\tc\n");
            assertViewStatus("vp", "valid", "", "2025-06-19T11:00:00.000000Z");

            drainViewQueue();
            assertViewStatus("vp", "valid", "", "2025-06-19T11:00:00.000000Z");

            execute("INSERT INTO t VALUES ('a', 3, 30), ('b', 4, 40)");
            setCurrentMicros(TRUNCATE_MICROS + 2 * busyRetryTimeoutMicros());
            drainViewQueue();
            assertViewStatus("vp", "valid", "", "2025-06-19T11:00:00.000000Z");
            assertViewColumnsOfQuery("vp", PIVOT_SQL);
            assertPendingCompileBusyRetries("");
            assertSqlCursors(PIVOT_SQL, "vp");
        });
    }

    @Test
    public void testTruncateWaitsAtMostBusyWaitTimeoutOverAllWaits() throws Exception {
        // The busy wait timeout bounds all waits of one TRUNCATE together. The test clock stands
        // still while TRUNCATE waits for a view compile run over t, and a query holds t meanwhile.
        // The second lock attempt of TRUNCATE closes the reader that the run left in the pool: at
        // that moment the clock moves by the whole timeout, and a run over t2 starts. The attempt
        // fails on the query while that run runs, and TRUNCATE gives up at once instead of waiting
        // for the run.
        assertMemoryLeak(() -> {
            final long timeoutMillis = 1_000;
            node1.setProperty(PropertyKey.CAIRO_WRITER_ALTER_BUSY_WAIT_TIMEOUT, timeoutMillis);
            createTableWithRows("t");
            createTableWithRows("t2");
            createView("v2", "SELECT * FROM t2");
            createViewsThenAddColumn();
            setCurrentMicros(ALTER_MICROS);

            final ViewCompileParker runOverT = ViewCompileParker.ofQueueDrain(engine, "t", PoolListener.SRC_READER);
            final ViewCompileParker runOverT2 = ViewCompileParker.ofQueueDrain(engine, "t2", PoolListener.SRC_READER);
            final AtomicInteger readerCloses = new AtomicInteger();
            final long ddlThread = Thread.currentThread().threadId();
            // runOverT.executeWhileHolding() passes each pool event to the run over t before this listener
            engine.setPoolListener((factoryType, thread, tableToken, event, segment, position) -> {
                runOverT2.onEvent(factoryType, thread, tableToken, event, segment, position);
                // Runs on the DDL thread, inside the second lock attempt, which reads the clock once
                // it fails. The attempt holds the lock of t's readers, so a run over t could not
                // borrow one: the run that starts here reads t2.
                if (thread == ddlThread
                        && factoryType == PoolListener.SRC_READER
                        && event == PoolListener.EV_LOCK_CLOSE
                        && Chars.equals(tableToken.getTableName(), "t")
                        && readerCloses.incrementAndGet() == 1) {
                    currentMicros += timeoutMillis * 1_000;
                    engine.enqueueCompileView(engine.verifyTableName("t2"));
                    startRun(runOverT2);
                }
            });
            // a TRUNCATE that waited past the timeout would wait for the parked run over t2
            // forever: release that run after a while, so that such a TRUNCATE ends and fails the
            // test
            final CountDownLatch isTruncateDone = new CountDownLatch(1);
            final Thread watchdog = new Thread(() -> {
                try {
                    if (!isTruncateDone.await(30, TimeUnit.SECONDS)) {
                        runOverT2.release();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            watchdog.start();
            try {
                runOverT.executeWhileHolding(capture, "t", () -> {
                    // The run holds t's reader in the pool's first slot, so the query takes the second
                    // slot. The first lock attempt of TRUNCATE fails on the run's slot, and the second
                    // one closes the reader that the run returned there before it fails on the query.
                    final ReaderHoldingThread query = ReaderHoldingThread.start(engine, "t");
                    try {
                        assertDdlFails("TRUNCATE TABLE t", "there is an active query against 't'. Try again.");
                        Assert.assertTrue(
                                "TRUNCATE waited for the run over t2",
                                ViewStateStore.isCompileRunRunning(engine.getViewStateStore().getCompileRunActivity())
                        );
                    } finally {
                        query.close();
                    }
                }, null);
            } finally {
                isTruncateDone.countDown();
                watchdog.join();
                runOverT2.join();
                engine.setPoolListener(null);
            }
            Assert.assertEquals(1, readerCloses.get());
            capture.drain();
            capture.assertOnlyOnce(Pattern.quote(WAIT_LINE));
            assertQuery("t")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            a\tb
                            1\tnull
                            2\tnull
                            """);

            // The failed second attempt kept the first slot of t's reader pool, as any failed lock
            // attempt keeps the slots it took. TRUNCATE on the same thread passes over the slot,
            // and its unlock drops t's pool entry with the slot.
            execute("TRUNCATE TABLE t");
            assertQuery("t")
                    .noLeakCheck()
                    .expectSize()
                    .returns("a\tb\n");
        });
    }

    @Test
    public void testTruncateWaitsUntilAllViewCompilesFinish() throws Exception {
        // Two view compile runs run at once, one holds t's reader and the other t2's reader.
        // TRUNCATE of t waits until no run is running, not only the one that holds t, so it
        // fails once the busy wait timeout passes while the run over t2 still runs.
        assertMemoryLeak(() -> {
            createTableWithRows("t");
            createView("v", "SELECT * FROM t");
            createTableWithRows("t2");
            createView("v2", "SELECT * FROM t2");
            final ViewCompileParker runOverT = ViewCompileParker.ofQueueDrain(engine, "t", PoolListener.SRC_READER);
            final ViewCompileParker runOverT2 = ViewCompileParker.ofQueueDrain(engine, "t2", PoolListener.SRC_READER);
            engine.setPoolListener(runOverT2);
            try {
                execute("ALTER TABLE t ADD COLUMN b INT");
                runOverT.executeWhileHolding(capture, "t", () -> {
                    execute("ALTER TABLE t2 ADD COLUMN b INT");
                    runOverT2.start();

                    // the server default
                    node1.setProperty(PropertyKey.CAIRO_WRITER_ALTER_BUSY_WAIT_TIMEOUT, 500);
                    assertDdlFailsAfter(500, "TRUNCATE TABLE t", "there is an active query against 't'. Try again.");
                }, null);
            } finally {
                runOverT2.join();
                engine.setPoolListener(null);
            }
            execute("TRUNCATE TABLE t");
            assertQuery("t")
                    .noLeakCheck()
                    .expectSize()
                    .returns("a\tb\n");
            drainViewQueue();
            assertViewState("v");
            assertViewState("v2");
            assertViewColumns("v", "a:INT, b:INT");
            assertViewColumns("v2", "a:INT, b:INT");
        });
    }

    @Test
    public void testTruncateWithoutOverlappingViewCompileKeepsViewStatus() throws Exception {
        // No view compile runs while TRUNCATE locks t's readers, so TRUNCATE queues no compile of
        // the views over t: they keep their status time. A compile over the emptied table would
        // mark the view whose PIVOT IN sub-query reads the rows of t invalid, and the INSERT that
        // follows queues no compile.
        assertMemoryLeak(() -> {
            setCurrentMicros(CREATE_MICROS);
            createPivotTable();
            createView("v", "SELECT * FROM t");
            createView("vp", PIVOT_SQL);
            setCurrentMicros(TRUNCATE_MICROS);
            execute("TRUNCATE TABLE t");
            drainViewQueue();
            assertViewStatus("v", "valid", "", "2025-06-19T10:00:00.000000Z");
            assertViewStatus("vp", "valid", "", "2025-06-19T10:00:00.000000Z");
            assertPendingCompileBusyRetries("");

            execute("INSERT INTO t VALUES ('a', 1), ('b', 2)");
            drainViewQueue();
            assertViewStatus("vp", "valid", "", "2025-06-19T10:00:00.000000Z");
            assertQuery("vp")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            a\tb
                            1\t2
                            """);
        });
    }

    @Test
    public void testWalDdlDoesNotWaitForViewCompile() throws Exception {
        // DROP, RENAME and TRUNCATE of a WAL table take no reader lock, so a view compile run that
        // holds the table's reader does not delay them.
        assertMemoryLeak(() -> {
            // the server default; a DDL that waited would log its wait
            node1.setProperty(PropertyKey.CAIRO_WRITER_ALTER_BUSY_WAIT_TIMEOUT, 500);
            execute("CREATE TABLE w (a INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO w VALUES (1, '2024-01-01T00:00:00.000000Z')");
            drainWalQueue();
            Assert.assertTrue(engine.verifyTableName("w").isWal());
            createView("vw", "SELECT * FROM w");
            engine.enqueueCompileView(engine.verifyTableName("w"));

            final ViewCompileParker run = ViewCompileParker.ofQueueDrain(engine, "w", PoolListener.SRC_READER);
            engine.setPoolListener(run);
            try {
                run.start();
                execute("TRUNCATE TABLE w");
                execute("RENAME TABLE w TO w2");
                execute("DROP TABLE w2");
            } finally {
                run.join();
                engine.setPoolListener(null);
            }
            drainWalQueue();
            capture.drain();
            capture.assertNotLogged(ViewCompileParker.DDL_WAIT_LINE_PREFIX);
            assertTableListed("w", false);
            assertTableListed("w2", false);
        });
    }

    // fails with the error of the action, or when the action did not run once for each table
    private static void assertActionRanOnceForEachTable(@Nullable Throwable actionError, int[] actionCounts, String[] tableNames) {
        if (actionError != null) {
            throw new AssertionError("action inside the lock failed", actionError);
        }
        for (int i = 0; i < tableNames.length; i++) {
            Assert.assertEquals("actions inside the lock of " + tableNames[i], 1, actionCounts[i]);
        }
    }

    private static void assertDdlFails(String ddl, String expectedError) {
        try {
            execute(ddl);
            Assert.fail("expected to fail: " + ddl);
        } catch (SqlException | CairoException e) {
            TestUtils.assertEquals(expectedError, e.getFlyweightMessage());
        }
    }

    // the DDL fails as expected, and not before the busy wait timeout passed
    private static void assertDdlFailsAfter(long timeoutMillis, String ddl, String expectedError) {
        final long startMillis = configuration.getMillisecondClock().getTicks();
        assertDdlFails(ddl, expectedError);
        final long elapsedMillis = configuration.getMillisecondClock().getTicks() - startMillis;
        Assert.assertTrue(ddl + " failed after " + elapsedMillis + "ms", elapsedMillis >= timeoutMillis);
    }

    private static void closeQuery(ReaderHoldingThread query) {
        try {
            query.close();
        } catch (InterruptedException e) {
            throw new AssertionError(e);
        }
    }

    // counts the DDL thread's locks of t's pooled metadata, one per lock attempt of DROP or RENAME
    private static AtomicInteger countTableMetadataLocks() {
        final AtomicInteger lockAttempts = new AtomicInteger();
        final long ddlThread = Thread.currentThread().threadId();
        engine.setPoolListener((factoryType, thread, tableToken, event, segment, position) -> {
            if (isTableMetadataLockOfT(ddlThread, factoryType, thread, tableToken, event)) {
                lockAttempts.incrementAndGet();
            }
        });
        return lockAttempts;
    }

    // runs the statements with an execution context of the calling thread
    private static void executeInOwnContext(String... sqls) throws SqlException {
        try (SqlExecutionContext executionContext = TestUtils.createSqlExecutionCtx(engine)) {
            for (String sql : sqls) {
                engine.execute(sql, executionContext);
            }
        }
    }

    // Runs the DDL while a view compile run holds the table from the pool. Once the DDL logs that
    // it waits for the run, the action runs and the run ends.
    private static void executeWhileViewCompileHolds(
            String tableName,
            byte poolSource,
            String ddl,
            @Nullable TestUtils.CheckedRunnable onWait
    ) throws Exception {
        ViewCompileParker.ofQueueDrain(engine, tableName, poolSource).executeWhileHolding(capture, "t", () -> execute(ddl), onWait);
    }

    // Runs the DDL, and the action on the DDL thread at its first event of the given kind on the
    // reader pool of each named table: at EV_LOCK_SUCCESS the DDL holds the lock of the table's
    // readers, at EV_LOCK_CLOSE it is trying to take the lock. Fails with the error of the action,
    // or when the action did not run once for each table.
    private static void executeWithActionInsideLock(
            String ddl,
            short lockEvent,
            Consumer<TableToken> action,
            String... tableNames
    ) throws SqlException {
        final long ddlThread = Thread.currentThread().threadId();
        final int[] actionCounts = new int[tableNames.length];
        final AtomicReference<Throwable> actionError = new AtomicReference<>();
        engine.setPoolListener((factoryType, thread, tableToken, event, segment, position) -> {
            if (thread == ddlThread && factoryType == PoolListener.SRC_READER && event == lockEvent) {
                for (int i = 0; i < tableNames.length; i++) {
                    if (Chars.equals(tableToken.getTableName(), tableNames[i]) && actionCounts[i]++ == 0) {
                        try {
                            action.accept(tableToken);
                        } catch (Throwable th) {
                            // an error thrown into the pool would leave the table locked
                            actionError.compareAndSet(null, th);
                        }
                    }
                }
            }
        });
        try {
            execute(ddl);
        } finally {
            engine.setPoolListener(null);
            assertActionRanOnceForEachTable(actionError.get(), actionCounts, tableNames);
        }
    }

    // Runs the DDL, and a drain of the view compile queue on another thread at the DDL thread's
    // first event of the given kind on the reader pool of each named table. The DDL thread waits
    // for the drain, so a compile of the drain that reads the table meets the lock.
    private static void executeWithViewCompileInsideLock(String ddl, short lockEvent, String... tableNames) throws SqlException {
        executeWithActionInsideLock(ddl, lockEvent, tableToken -> ViewCompileParker.runQueueDrain(engine), tableNames);
    }

    private static boolean isPresentInDbRoot(String fileName) {
        try (Path path = new Path()) {
            return configuration.getFilesFacade().exists(path.of(configuration.getDbRoot()).concat(fileName).$());
        }
    }

    private static boolean isTableMetadataLockOfT(long ddlThread, byte factoryType, long thread, TableToken tableToken, short event) {
        return thread == ddlThread
                && factoryType == PoolListener.SRC_TABLE_METADATA
                && event == PoolListener.EV_LOCK_SUCCESS
                && Chars.equals(tableToken.getTableName(), "t");
    }

    private static boolean isTableMetadataUnlockOfT(long ddlThread, byte factoryType, long thread, TableToken tableToken, short event) {
        return thread == ddlThread
                && factoryType == PoolListener.SRC_TABLE_METADATA
                && event == PoolListener.EV_UNLOCKED
                && Chars.equals(tableToken.getTableName(), "t");
    }

    private static void joinRun(ViewCompileParker run) {
        try {
            run.join();
        } catch (InterruptedException e) {
            throw new AssertionError(e);
        }
    }

    // records a borrow or a return of the reader of t or t2, as "t:borrow " or "t2:return "
    private static void recordReaderEvent(StringBuffer events, TableToken tableToken, short event) {
        if (tableToken != null && (Chars.equals(tableToken.getTableName(), "t") || Chars.equals(tableToken.getTableName(), "t2"))) {
            if (event == PoolListener.EV_GET || event == PoolListener.EV_CREATE) {
                events.append(tableToken.getTableName()).append(":borrow ");
            } else if (event == PoolListener.EV_RETURN) {
                events.append(tableToken.getTableName()).append(":return ");
            }
        }
    }

    private static void startRun(ViewCompileParker run) {
        try {
            run.start();
        } catch (InterruptedException e) {
            throw new AssertionError(e);
        }
    }

    private void assertDdlAfterTableGoneDuringViewCompileWait(
            String ddl,
            @Nullable String expectedError,
            boolean isNameReserved
    ) throws Exception {
        final TableToken[] reservedName = new TableToken[1];
        try {
            executeWhileQueryAndViewCompileHold(ddl, () -> {
                ViewCompileParker.dropTableAsAnotherSession(engine, "t", false);
                if (isNameReserved) {
                    reservedName[0] = engine.lockTableName("t");
                    Assert.assertNotNull(reservedName[0]);
                }
            });
            Assert.assertNull("expected to fail: " + ddl, expectedError);
        } catch (SqlException | CairoException e) {
            Assert.assertNotNull("unexpected failure of " + ddl + ": " + e.getFlyweightMessage(), expectedError);
            TestUtils.assertEquals(expectedError, e.getFlyweightMessage());
        } finally {
            if (reservedName[0] != null) {
                engine.unlockTableName(reservedName[0]);
            }
        }
        assertTableListed("t", false);
        assertTableListed("u", false);
        Assert.assertFalse(isPresentInDbRoot("t~"));
        Assert.assertFalse(isPresentInDbRoot("u~"));
    }

    // The retry callback runs on the DDL thread after it waited for a view compile run and found
    // t under its name. The callback has another session drop t, and create a new t when asked.
    private void assertDdlDoesNotActOnTableChangedBeforeRetryLock(String ddl, String expectedError, boolean isRecreated) throws Exception {
        final AtomicInteger retries = new AtomicInteger();
        engine.setViewCompileRetryCallback(() -> {
            if (retries.incrementAndGet() == 1) {
                final AtomicReference<Throwable> sessionError = new AtomicReference<>();
                final Thread session = new Thread(() -> {
                    try {
                        ViewCompileParker.dropTableAsAnotherSession(engine, "t", isRecreated);
                    } catch (Throwable th) {
                        sessionError.set(th);
                    } finally {
                        Path.clearThreadLocals();
                    }
                });
                session.start();
                try {
                    session.join();
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                }
                if (sessionError.get() != null) {
                    throw new AssertionError("could not change t", sessionError.get());
                }
            }
        });
        try {
            executeWhileQueryAndViewCompileHold(ddl, null);
            Assert.fail("expected to fail: " + ddl);
        } catch (SqlException | CairoException e) {
            TestUtils.assertEquals(expectedError, e.getFlyweightMessage());
        } finally {
            engine.setViewCompileRetryCallback(null);
        }
        Assert.assertEquals(1, retries.get());
        Assert.assertEquals(0, engine.getBusyReaderCount());
        Assert.assertEquals(0, engine.getBusyWriterCount());
        assertTableListed("u", false);
        Assert.assertFalse(isPresentInDbRoot("u~"));
        if (isRecreated) {
            assertQuery("t")
                    .noLeakCheck()
                    .expectSize()
                    .returns("a\n42\n");
            Assert.assertTrue(isPresentInDbRoot("t~"));

            // the statement run again resolves the new table
            execute(ddl);
            assertTableListed("t", false);
            if (ddl.startsWith("RENAME")) {
                assertQuery("u")
                        .noLeakCheck()
                        .expectSize()
                        .returns("a\n42\n");
                execute("DROP TABLE u");
            }
        } else {
            assertTableListed("t", false);
            Assert.assertFalse(isPresentInDbRoot("t~"));
            Assert.assertFalse(isPresentInDbRoot("t~.lock"));
            // the name is free
            execute("CREATE TABLE t (a INT)");
            execute("DROP TABLE t");
        }
    }

    private void assertDdlDoesNotActOnTableReplacedDuringViewCompileWait(String ddl, String expectedError) throws Exception {
        try {
            executeWhileQueryAndViewCompileHold(ddl, () -> ViewCompileParker.dropTableAsAnotherSession(engine, "t", true));
            Assert.fail("expected to fail: " + ddl);
        } catch (SqlException | CairoException e) {
            TestUtils.assertEquals(expectedError, e.getFlyweightMessage());
        }
        assertQuery("t")
                .noLeakCheck()
                .expectSize()
                .returns("a\n42\n");
        assertTableListed("u", false);
        Assert.assertTrue(isPresentInDbRoot("t~"));

        // the statement run again resolves the new table
        execute(ddl);
        assertTableListed("t", false);
        if (ddl.startsWith("RENAME")) {
            assertQuery("u")
                    .noLeakCheck()
                    .expectSize()
                    .returns("a\n42\n");
            execute("DROP TABLE u");
        }
    }

    // Runs the DDLs while a view compile run holds t's reader. A DDL that waited for the run would
    // log its wait before it fails, so the log must not carry the wait line.
    private void assertDdlDoesNotWaitForViewCompile(TestUtils.LeakProneCode ddls) throws Exception {
        // the server default
        node1.setProperty(PropertyKey.CAIRO_WRITER_ALTER_BUSY_WAIT_TIMEOUT, 500);
        createTableAndViewsThenAddColumn();
        final ViewCompileParker run = ViewCompileParker.ofQueueDrain(engine, "t", PoolListener.SRC_READER);
        engine.setPoolListener(run);
        try {
            run.start();
            ddls.run();
        } finally {
            run.join();
            engine.setPoolListener(null);
        }
        capture.drain();
        capture.assertNotLogged(WAIT_LINE);

        execute("TRUNCATE TABLE t");
        execute("RENAME TABLE t TO u");
        execute("DROP TABLE u");
        assertTableListed("t", false);
        assertTableListed("u", false);
    }

    // The listener starts a view compile run on the DDL thread's first lock of t's metadata, and
    // waits until the run holds t's reader. It releases the run, and waits for it to end, when
    // the failed lock attempt unlocks t's metadata.
    private void assertDdlRetriesWhenViewCompileStartsAndEndsInsideLockAttempt(String ddl) throws Exception {
        final ViewCompileParker run = ViewCompileParker.ofQueueDrain(engine, "t", PoolListener.SRC_READER);
        final AtomicInteger lockAttempts = new AtomicInteger();
        final AtomicInteger unlocks = new AtomicInteger();
        final long ddlThread = Thread.currentThread().threadId();
        engine.setPoolListener((factoryType, thread, tableToken, event, segment, position) -> {
            run.onEvent(factoryType, thread, tableToken, event, segment, position);
            if (isTableMetadataLockOfT(ddlThread, factoryType, thread, tableToken, event)
                    && lockAttempts.incrementAndGet() == 1) {
                startRun(run);
            } else if (isTableMetadataUnlockOfT(ddlThread, factoryType, thread, tableToken, event)
                    && unlocks.incrementAndGet() == 1) {
                joinRun(run);
            }
        });
        try {
            execute(ddl);
        } finally {
            run.join();
            engine.setPoolListener(null);
        }
        Assert.assertEquals(2, lockAttempts.get());
    }

    private void assertTableListed(String tableName, boolean isListed) throws Exception {
        assertQuery("SELECT table_name FROM tables() WHERE table_name = '" + tableName + "'")
                .noLeakCheck()
                .noRandomAccess()
                .returns(isListed ? "table_name\n" + tableName + '\n' : "table_name\n");
    }

    // The compiles of the views v and v_dep met the lock of t's readers at lockMicros. They left
    // the views valid as their creation at CREATE_MICROS made them, with the given columns, and each
    // view has one busy retry of the compile that the ALTER, or COMPILE VIEW, queued at ALTER_MICROS.
    private void assertViewsKeptStateWithBusyRetries(String columns, long lockMicros) throws Exception {
        assertViewStatus("v", "valid", "", "2025-06-19T10:00:00.000000Z");
        assertViewStatus("v_dep", "valid", "", "2025-06-19T10:00:00.000000Z");
        assertViewColumns("v", columns);
        assertViewColumns("v_dep", columns);
        final long dueMicros = lockMicros + busyRetryTimeoutMicros();
        assertPendingCompileBusyRetries(
                pendingCompileBusyRetry("v", ALTER_MICROS, dueMicros)
                        + pendingCompileBusyRetry("v_dep", ALTER_MICROS, dueMicros)
        );
    }

    // the busy retries ran: the views v and v_dep are valid with the given columns, at ALTER_MICROS,
    // and no retry is pending
    private void assertViewsValidAtAlterTime(String columns) throws Exception {
        assertViewStatus("v", "valid", "", "2025-06-19T11:00:00.000000Z");
        assertViewStatus("v_dep", "valid", "", "2025-06-19T11:00:00.000000Z");
        assertViewColumns("v", columns);
        assertViewColumns("v_dep", columns);
        assertPendingCompileBusyRetries("");
    }

    // creates a non-partitioned table, which is never a WAL table, and a view over it at CREATE_MICROS
    private void createTableAndView(String tableName, String viewName) throws SqlException {
        setCurrentMicros(CREATE_MICROS);
        execute("CREATE TABLE " + tableName + " (a INT)");
        execute("INSERT INTO " + tableName + " VALUES (1), (2)");
        createView(viewName, "SELECT * FROM " + tableName);
        assertViewColumns(viewName, "a:INT");
        Assert.assertFalse(engine.verifyTableName(tableName).isWal());
    }

    // creates the non-WAL table t with rows 1 and 2, and the views over it, then adds a column to t
    private void createTableAndViewsThenAddColumn() throws SqlException {
        createTableWithRows("t");
        createViewsThenAddColumn();
    }

    // creates the non-WAL table t with rows 1 and 2 and the views over it at CREATE_MICROS, then adds
    // a column to t at ALTER_MICROS
    private void createTableAndViewsThenAddColumnAtAlterTime() throws SqlException {
        setCurrentMicros(CREATE_MICROS);
        createTableWithRows("t");
        createViewsThenAddColumnAtAlterTime();
    }

    // creates the view v over table t and the view v_dep over v, then adds column b to t, which
    // queues the recompile of both views
    private void createViewsThenAddColumn() throws SqlException {
        createViewsOverT();
        execute("ALTER TABLE t ADD COLUMN b INT");
    }

    // creates the views over t at CREATE_MICROS, then adds column b to t at ALTER_MICROS, the update
    // timestamp of the recompile that the ALTER queues
    private void createViewsThenAddColumnAtAlterTime() throws SqlException {
        setCurrentMicros(CREATE_MICROS);
        createViewsOverT();
        setCurrentMicros(ALTER_MICROS);
        execute("ALTER TABLE t ADD COLUMN b INT");
    }

    // creates the view v over the non-WAL table t and the view v_dep over v
    private void createViewsOverT() throws SqlException {
        Assert.assertFalse(engine.verifyTableName("t").isWal());
        createView("v", "SELECT * FROM t");
        createView("v_dep", "SELECT * FROM v");
    }

    // Creates t with the rows 1 and 2 and runs the DDL of t while a query holds t's reader and a
    // view compile run holds t2's reader, so that the DDL fails on the query and waits for the run.
    // Once the DDL logs that it waits, the query ends, the action runs and the run ends.
    private void executeWhileQueryAndViewCompileHold(String ddl, @Nullable TestUtils.CheckedRunnable onWait) throws Exception {
        ViewCompileParker.executeWhileQueryAndViewCompileHold(engine, sqlExecutionContext, capture, addedColumnCount++, () -> execute(ddl), onWait);
    }
}
