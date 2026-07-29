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

package io.questdb.test.cairo.mv;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoTable;
import io.questdb.cairo.MetadataCacheReader;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.RowExpiryCleanupJob;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.file.BlockFileReader;
import io.questdb.cairo.mv.MatViewDefinition;
import io.questdb.cairo.sql.InsertOperation;
import io.questdb.cairo.sql.OperationFuture;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.cairo.view.ViewDefinition;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.mp.Job;
import io.questdb.std.LowerCaseCharSequenceHashSet;
import io.questdb.std.Misc;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Verifies the EXPIRE ROWS row-expiry clause on MATERIALIZED VIEWs:
 * <ul>
 *     <li>{@code CREATE MATERIALIZED VIEW ... EXPIRE ROWS WHEN <pred> [CLEANUP EVERY <dur>]}</li>
 *     <li>{@code ALTER MATERIALIZED VIEW ... SET EXPIRE ROWS WHEN <pred> [CLEANUP EVERY <dur>]}</li>
 *     <li>{@code ALTER MATERIALIZED VIEW ... DROP EXPIRE}</li>
 * </ul>
 * Mat views are WAL tables, so the _meta persistence and the read-time row-expiry filter are shared
 * with plain tables; the filter is materialized-view-only ({@code isMatView()}), excluding plain tables
 * and plain views alike.
 * These tests confirm the grammar/threading and that querying a policied mat view hides expired rows.
 * EXPIRE ROWS is allowed on an aggregating (SAMPLE BY) view too, advisory only: reads stay correct via the
 * read filter, but physical reclamation is best-effort since a later refresh can regenerate reclaimed rows
 * ({@link #testCreateAggregatingMatViewWithExpireAllowed()}).
 */
public class MatViewExpireRowsTest extends AbstractCairoTest {

    @Before
    public void setUp() {
        super.setUp();
        // Mat views are gated behind dev mode, exactly as MatViewTest enables them.
        setProperty(PropertyKey.DEV_MODE_ENABLED, "true");
    }

    @Test
    public void testCachedPlanCompiledDuringDropExpireUsesCurrentPolicy() throws Exception {
        assertCachedPlanCompiledDuringPolicyChange(
                " EXPIRE ROWS WHEN v < 2.0",
                """
                        k\tv
                        B\t2.0
                        C\t3.0
                        """,
                "ALTER MATERIALIZED VIEW mv DROP EXPIRE",
                """
                        k\tv
                        A\t1.0
                        B\t2.0
                        C\t3.0
                        """
        );
    }

    @Test
    public void testCachedPlanCompiledDuringFirstSetExpireUsesCurrentPolicy() throws Exception {
        assertCachedPlanCompiledDuringPolicyChange(
                "",
                """
                        k\tv
                        A\t1.0
                        B\t2.0
                        C\t3.0
                        """,
                "ALTER MATERIALIZED VIEW mv SET EXPIRE ROWS WHEN v < 2.0",
                """
                        k\tv
                        B\t2.0
                        C\t3.0
                        """
        );
    }

    @Test
    public void testCachedPlanCompiledDuringReplacementSetExpireUsesCurrentPolicy() throws Exception {
        assertCachedPlanCompiledDuringPolicyChange(
                " EXPIRE ROWS WHEN v < 2.0",
                """
                        k\tv
                        B\t2.0
                        C\t3.0
                        """,
                "ALTER MATERIALIZED VIEW mv SET EXPIRE ROWS WHEN v < 3.0",
                """
                        k\tv
                        C\t3.0
                        """
        );
    }

    @Test
    public void testCachedPlanCompiledInExpirePublicationWindowHidesExpiredRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (k SYMBOL, v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO base VALUES
                    ('A', 1.0, '2024-01-01T00:00:00.000000Z'),
                    ('B', 2.0, '2024-01-02T00:00:00.000000Z'),
                    ('C', 3.0, '2024-01-03T00:00:00.000000Z')
                    """);
            execute("CREATE MATERIALIZED VIEW mv AS (SELECT * FROM base)");
            drainWalAndMatViewQueues();

            // Before any policy, every row is visible. This also warms the metadata cache.
            assertQuery("SELECT k, v FROM mv ORDER BY k").expectSize().noLeakCheck().returns("""
                    k\tv
                    A\t1.0
                    B\t2.0
                    C\t3.0
                    """);

            final AtomicReference<Throwable> applyError = new AtomicReference<>();
            final AtomicReference<Throwable> compileError = new AtomicReference<>();
            final AtomicReference<RecordCursorFactory> compiledFactory = new AtomicReference<>();

            final CountDownLatch swapBarrierReached = new CountDownLatch(1);
            final CountDownLatch resumeSwap = new CountDownLatch(1);
            final CountDownLatch readerGateReached = new CountDownLatch(1);
            final CountDownLatch resumeReaderOpen = new CountDownLatch(1);
            final CountDownLatch publishBarrierReached = new CountDownLatch(1);
            final CountDownLatch resumePublish = new CountDownLatch(1);

            execute("ALTER MATERIALIZED VIEW mv SET EXPIRE ROWS WHEN v < 2.0");

            // The WAL-apply writer marks the change as pending, then pauses (a) just before the _meta/_txn
            // swap and (b) just after the swap but before the policy epoch counter ticks the second time.
            TableWriter.setExpiryMetaSwapBarrier(() -> {
                swapBarrierReached.countDown();
                awaitOrThrow(resumeSwap, "resume the _meta/_txn swap");
            });
            TableWriter.setExpiryPolicyPublishBarrier(() -> {
                publishBarrierReached.countDown();
                awaitOrThrow(resumePublish, "resume the policy epoch publish");
            });

            // This context makes the compiler wait at the point where it opens the mv reader, so the parse
            // reads the old (no-policy) metadata before the swap and the reader opens the new metadata after it.
            final SqlExecutionContextImpl gatedCtx = new SqlExecutionContextImpl(engine, 1) {
                private boolean gateArmed = true;

                @Override
                public TableReader getReader(TableToken tableToken) {
                    if (gateArmed && "mv".contentEquals(tableToken.getTableName())) {
                        gateArmed = false;
                        readerGateReached.countDown();
                        awaitOrThrow(resumeReaderOpen, "resume the mv reader open");
                    }
                    return getCairoEngine().getReader(tableToken, getReaderPoolSupervisor());
                }
            };
            gatedCtx.with(engine.getConfiguration().getFactoryProvider().getSecurityContextFactory().getRootContext(), null);

            final Thread applyThread = new Thread(() -> {
                try {
                    drainWalQueue();
                } catch (Throwable th) {
                    applyError.set(th);
                } finally {
                    Path.clearThreadLocals();
                }
            }, "expire-apply");

            final Thread compileThread = new Thread(() -> {
                try {
                    compiledFactory.set(select("SELECT k, v FROM mv ORDER BY k", gatedCtx));
                } catch (Throwable th) {
                    compileError.set(th);
                } finally {
                    Path.clearThreadLocals();
                }
            }, "expire-compile");

            try {
                applyThread.start();
                assertTrue("writer did not reach the pre-swap barrier", swapBarrierReached.await(30, TimeUnit.SECONDS));
                // The change is pending and the counter has ticked once, but the on-disk metadata is still the
                // old no-policy version.

                compileThread.start();
                assertTrue("compiler did not reach the mv reader gate", readerGateReached.await(30, TimeUnit.SECONDS));
                // Parse read the old no-policy metadata, so the plan so far has no keep-filter.

                resumeSwap.countDown();
                assertTrue("writer did not reach the pre-publish barrier", publishBarrierReached.await(30, TimeUnit.SECONDS));
                // The on-disk metadata now has the policy, but the policy epoch counter has not ticked the
                // second time yet.

                resumeReaderOpen.countDown();
                compileThread.join(TimeUnit.SECONDS.toMillis(30));
                assertFalse("compile did not finish", compileThread.isAlive());
                if (compileError.get() != null) {
                    throw new AssertionError("compile failed", compileError.get());
                }

                resumePublish.countDown();
                applyThread.join(TimeUnit.SECONDS.toMillis(30));
                assertFalse("WAL apply did not finish", applyThread.isAlive());
                if (applyError.get() != null) {
                    throw new AssertionError("WAL apply failed", applyError.get());
                }
                drainWalAndMatViewQueues();

                // The cached plan must apply the new policy: row A (v < 2.0) has expired and must not appear.
                try (RecordCursorFactory factory = compiledFactory.getAndSet(null)) {
                    assertNotNull("compiler produced no factory", factory);
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        assertCursor("""
                                k\tv
                                B\t2.0
                                C\t3.0
                                """, cursor, factory.getMetadata(), true);
                    }
                }
            } finally {
                resumeSwap.countDown();
                resumeReaderOpen.countDown();
                resumePublish.countDown();
                TableWriter.setExpiryMetaSwapBarrier(null);
                TableWriter.setExpiryPolicyPublishBarrier(null);
                applyThread.join(TimeUnit.SECONDS.toMillis(30));
                compileThread.join(TimeUnit.SECONDS.toMillis(30));
                Misc.free(compiledFactory.get());
                Misc.free(gatedCtx);
            }
        });
    }

    @Test
    public void testInsertSelectCompiledDuringDropExpireUsesCurrentPolicy() throws Exception {
        assertInsertSelectCompiledDuringPolicyChange(
                " EXPIRE ROWS WHEN v < 2.0",
                "ALTER MATERIALIZED VIEW mv DROP EXPIRE",
                """
                        k\tv
                        A\t1.0
                        B\t2.0
                        C\t3.0
                        """
        );
    }

    @Test
    public void testInsertSelectCompiledDuringFirstSetExpireUsesCurrentPolicy() throws Exception {
        assertInsertSelectCompiledDuringPolicyChange(
                "",
                "ALTER MATERIALIZED VIEW mv SET EXPIRE ROWS WHEN v < 2.0",
                """
                        k\tv
                        B\t2.0
                        C\t3.0
                        """
        );
    }

    @Test
    public void testInsertSelectCompiledDuringReplacementSetExpireUsesCurrentPolicy() throws Exception {
        assertInsertSelectCompiledDuringPolicyChange(
                " EXPIRE ROWS WHEN v < 2.0",
                "ALTER MATERIALIZED VIEW mv SET EXPIRE ROWS WHEN v < 3.0",
                """
                        k\tv
                        C\t3.0
                        """
        );
    }

    @Test
    public void testAlterViewCompiledDuringDropExpireUsesCurrentDependencies() throws Exception {
        assertViewCompiledDuringPolicyChange(
                " EXPIRE ROWS WHEN v < 2.0",
                "CREATE VIEW v1 AS (SELECT k FROM mv)",
                "ALTER MATERIALIZED VIEW mv DROP EXPIRE",
                "ALTER VIEW v1 AS (SELECT k FROM mv)",
                "k\nA\nB\nC\n",
                null,
                "v"
        );
    }

    @Test
    public void testCreateOrReplaceViewCompiledDuringReplacementSetExpireUsesCurrentDependencies() throws Exception {
        assertViewCompiledDuringPolicyChange(
                " EXPIRE ROWS WHEN v < 2.0",
                "CREATE VIEW v1 AS (SELECT k FROM mv)",
                "ALTER MATERIALIZED VIEW mv SET EXPIRE ROWS WHEN ts < '2024-01-03'",
                "CREATE OR REPLACE VIEW v1 AS (SELECT k FROM mv)",
                "k\nC\n",
                "ts",
                "v"
        );
    }

    @Test
    public void testCreateViewCompiledDuringFirstSetExpireUsesCurrentDependencies() throws Exception {
        assertViewCompiledDuringPolicyChange(
                "",
                null,
                "ALTER MATERIALIZED VIEW mv SET EXPIRE ROWS WHEN v < 2.0",
                "CREATE VIEW v1 AS (SELECT k FROM mv)",
                "k\nB\nC\n",
                "v",
                "ts"
        );
    }

    @Test
    public void testCachedPlanInvalidatedAcrossSetAndDropExpire() throws Exception {
        // A cached/prepared SELECT factory compiled before a policy exists must not keep returning expired
        // rows after SET EXPIRE: the metadata-version bump invalidates it, so the next execution throws
        // TableReferenceOutOfDateException and the caller recompiles with the read filter applied. DROP EXPIRE
        // invalidates it again, restoring the unfiltered plan. Mirrors the pgwire/http prepared-statement cache
        // recompile-on-stale contract across policy transitions.
        assertMemoryLeak(() -> {
            execute("create table base (k symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("""
                    insert into base values
                    ('A', 1.0, '2024-01-01T00:00:00.000000Z'),
                    ('B', 2.0, '2024-01-02T00:00:00.000000Z'),
                    ('C', 3.0, '2024-01-03T00:00:00.000000Z')""");
            execute("create materialized view mv as (select * from base)");
            drainWalAndMatViewQueues();

            try (RecordCursorFactory factory = select("select k, v from mv order by k")) {
                // Before any policy: the cached plan sees every row.
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    assertCursor("k\tv\nA\t1.0\nB\t2.0\nC\t3.0\n", cursor, factory.getMetadata(), true);
                }

                // SET a policy: the metadata version bumps, so the cached plan is now stale and must not run.
                execute("alter materialized view mv set expire rows when v < 2.0");
                drainWalAndMatViewQueues();
                try (RecordCursor ignore = factory.getCursor(sqlExecutionContext)) {
                    org.junit.Assert.fail("cached plan must be invalidated by SET EXPIRE");
                } catch (TableReferenceOutOfDateException expected) {
                    // expected -- the caller must recompile
                }
            }

            // A recompiled read applies the filter: v < 2.0 is expired, so A is hidden; B and C remain.
            assertQuery("select k, v from mv order by k").noLeakCheck().returns("k\tv\nB\t2.0\nC\t3.0\n");

            // DROP the policy: the cached plan is invalidated again; a recompiled read is unfiltered.
            execute("alter materialized view mv drop expire");
            drainWalAndMatViewQueues();
            assertQuery("select k, v from mv order by k").sizeMayVary().noLeakCheck().returns("k\tv\nA\t1.0\nB\t2.0\nC\t3.0\n");
        });
    }

    @Test
    public void testCountReflectsKeepLatestReadFilter() throws Exception {
        // All rows land in one (active) partition that cleanup never touches, so a superseded row stays on
        // disk. count() must still reflect the read filter (one latest row per key), not the raw row count.
        assertMemoryLeak(() -> {
            execute("create table base (k symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into base values " +
                    "('A', 1.0, '2024-01-01T00:00:00.000000Z')," +   // superseded (non-active partition)
                    "('A', 2.0, '2024-01-02T00:00:00.000000Z')," +   // superseded (non-active partition)
                    "('B', 3.0, '2024-01-01T00:00:00.000000Z')," +   // superseded (non-active partition)
                    "('A', 4.0, '2024-01-03T00:00:00.000000Z')," +   // latest A (active partition)
                    "('B', 5.0, '2024-01-02T00:00:00.000000Z')");    // latest B (non-active partition)
            drainWalAndMatViewQueues();
            execute("create materialized view mv as (select * from base) expire rows keep latest partition by k");
            drainWalAndMatViewQueues();
            // NO cleanup: all 5 rows are physically present across 3 partitions. The read filter must still
            // show exactly the latest row per key, and count() must agree.
            assertQuery("select k, v from mv order by k").expectSize().noLeakCheck().returns("k\tv\nA\t4.0\nB\t5.0\n");
            assertQuery("select count() c from mv").noRandomAccess().expectSize().noLeakCheck().returns("c\n2\n");
            assertQuery("select count(distinct k) c from mv").noRandomAccess().expectSize().noLeakCheck().returns("c\n2\n");
        });
    }

    @Test
    public void testExpireScalarCustomCleanupCompactsAndWipes() throws Exception {
        // A non-time (custom) scalar predicate has no bounds fast-path, so cleanup classifies via the count
        // scan: a fully-expired partition is wiped, a partial one compacted.
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into base values " +
                    "('A', 1.0, '2024-01-01T00:00:00.000000Z')," +   // v<2 -> expired (d1 partial)
                    "('B', 5.0, '2024-01-01T00:00:00.000000Z')," +   // kept
                    "('C', 1.5, '2024-01-02T00:00:00.000000Z')," +   // v<2 -> d2 fully expired
                    "('D', 9.0, '2024-01-03T00:00:00.000000Z')");    // active partition
            drainWalAndMatViewQueues();
            execute("create materialized view mv as (select * from base) expire rows when v < 2.0");
            drainWalAndMatViewQueues();
            assertQuery("select count() p, sum(numRows) r from table_partitions('mv')").noRandomAccess().expectSize().noLeakCheck().returns("p\tr\n3\t4\n");

            final TableToken token = engine.verifyTableName("mv");
            final String predicate;
            try (TableMetadata m = engine.getTableMetadata(token)) {
                predicate = m.getExpiryPredicate();
            }
            try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
                job.cleanupTable(token, predicate);
            }
            drainWalAndMatViewQueues();

            assertQuery("select count() p, sum(numRows) r from table_partitions('mv')").noRandomAccess().expectSize().noLeakCheck().returns("p\tr\n2\t2\n");
            assertQuery("select sym, v from mv order by sym").noLeakCheck().returns("""
                    sym\tv
                    B\t5.0
                    D\t9.0
                    """);
        });
    }

    @Test
    public void testExpireScalarCleanupKeepsNullPredicateRow() throws Exception {
        // A row whose scalar predicate operand is NULL is KEPT (v < 2.0 is UNKNOWN, not TRUE). Physical
        // cleanup must not delete it: the partial partition compacts but retains the NULL row. Scalar-WHEN
        // cleanup uses its own keep-filter (buildRowExpiryKeepFilter), so this guards that path's 3VL.
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into base values " +
                    "('A', 1.0, '2024-01-01T00:00:00.000000Z')," +   // v<2 -> expired (d1 partial)
                    "('C', null, '2024-01-01T00:00:00.000000Z')," +  // NULL -> kept (d1)
                    "('B', 1.5, '2024-01-02T00:00:00.000000Z')," +   // v<2 -> d2 fully expired
                    "('D', 9.0, '2024-01-03T00:00:00.000000Z')");    // active partition
            drainWalAndMatViewQueues();
            execute("create materialized view mv as (select * from base) expire rows when v < 2.0");
            drainWalAndMatViewQueues();
            assertQuery("select count() p, sum(numRows) r from table_partitions('mv')").noRandomAccess().expectSize().noLeakCheck().returns("p\tr\n3\t4\n");

            final TableToken token = engine.verifyTableName("mv");
            final String predicate;
            try (TableMetadata m = engine.getTableMetadata(token)) {
                predicate = m.getExpiryPredicate();
            }
            try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
                job.cleanupTable(token, predicate);
            }
            drainWalAndMatViewQueues();

            // d1 partial (A expired, C null kept) -> 1 row; d2 fully expired -> wiped; active d3 kept.
            assertQuery("select count() p, sum(numRows) r from table_partitions('mv')").noRandomAccess().expectSize().noLeakCheck().returns("p\tr\n2\t2\n");
            assertQuery("select sym, v from mv order by sym").noLeakCheck().returns("""
                    sym\tv
                    C\tnull
                    D\t9.0
                    """);
        });
    }

    @Test
    public void testExpireScalarCleanupHourlyPartitionsCompactAndWipe() throws Exception {
        // Physical reclamation on PARTITION BY HOUR: partition floors and bounds are hourly, so the sweep
        // must wipe a fully-expired hour, compact a partial hour to its survivors, and leave a kept row
        // sitting exactly on the next hour's floor untouched by the previous hour's range.
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by hour wal");
            execute("insert into base values " +
                    "('A', 1.0, '2024-01-05T00:15:00.000000Z')," +  // v<2 -> h00 fully expired -> wiped
                    "('B', 1.5, '2024-01-05T01:20:00.000000Z')," +  // v<2 -> expired (h01 partial)
                    "('C', 5.0, '2024-01-05T01:40:00.000000Z')," +  // kept (h01 survivor)
                    "('D', 6.0, '2024-01-05T02:00:00.000000Z')," +  // kept; exactly on the h02 floor
                    "('E', 9.0, '2024-01-05T03:00:00.000000Z')");   // active partition
            drainWalAndMatViewQueues();
            // The passthrough view mirrors the base's HOUR partitioning.
            execute("create materialized view mv as (select * from base) expire rows when v < 2.0");
            drainWalAndMatViewQueues();
            assertQuery("select count() p, sum(numRows) r from table_partitions('mv')").noRandomAccess().expectSize().noLeakCheck().returns("p\tr\n4\t5\n");

            final TableToken token = engine.verifyTableName("mv");
            final String predicate;
            try (TableMetadata m = engine.getTableMetadata(token)) {
                predicate = m.getExpiryPredicate();
            }
            final boolean reclaimed;
            try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
                reclaimed = job.cleanupTable(token, predicate);
            }
            assertTrue("hourly sweep must reclaim", reclaimed);
            drainWalAndMatViewQueues();

            // h00 wiped; h01 compacted to C; the h02 boundary row and the active h03 stay untouched.
            assertQuery("select count() p, sum(numRows) r from table_partitions('mv')").noRandomAccess().expectSize().noLeakCheck().returns("p\tr\n3\t3\n");
            assertQuery("select sym, v from mv order by sym").noLeakCheck().returns("""
                    sym\tv
                    C\t5.0
                    D\t6.0
                    E\t9.0
                    """);
        });
    }

    @Test
    public void testExpireScalarCleanupNanoTimestampCompactsAndWipes() throws Exception {
        // A TIMESTAMP_NS designated timestamp has no partition-bounds fast path, so cleanup classifies via
        // the survivor count scan. The scan's partition-range bind variables must carry the column's native
        // unit (nanos): a partial partition compacts to its survivors, a fully-expired one is wiped.
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, v double, ts timestamp_ns) timestamp(ts) partition by day wal");
            execute("insert into base values " +
                    "('A', 1.0, '2024-01-01T00:00:00.000000000Z')," +   // v<2 -> expired (d1 partial)
                    "('B', 5.0, '2024-01-01T00:00:00.000000000Z')," +   // kept
                    "('C', 1.5, '2024-01-02T00:00:00.000000000Z')," +   // v<2 -> d2 fully expired
                    "('D', 9.0, '2024-01-03T00:00:00.000000000Z')");    // active partition
            drainWalAndMatViewQueues();
            execute("create materialized view mv as (select * from base) expire rows when v < 2.0");
            drainWalAndMatViewQueues();
            assertQuery("select count() p, sum(numRows) r from table_partitions('mv')").noRandomAccess().expectSize().noLeakCheck().returns("p\tr\n3\t4\n");

            final TableToken token = engine.verifyTableName("mv");
            final String predicate;
            try (TableMetadata m = engine.getTableMetadata(token)) {
                predicate = m.getExpiryPredicate();
            }
            try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
                job.cleanupTable(token, predicate);
            }
            drainWalAndMatViewQueues();

            assertQuery("select count() p, sum(numRows) r from table_partitions('mv')").noRandomAccess().expectSize().noLeakCheck().returns("p\tr\n2\t2\n");
            assertQuery("select sym, v from mv order by sym").noLeakCheck().returns("""
                    sym\tv
                    B\t5.0
                    D\t9.0
                    """);
        });
    }

    @Test
    public void testExpireScalarCleanupNanoTimestampKeepsLiveRows() throws Exception {
        // Cleanup on a TIMESTAMP_NS view must never remove live rows. Nothing satisfies v < 0 here, so a
        // sweep must leave every partition intact. The survivor scan's partition-range bind variables carry
        // nano floors typed as nanos; a unit mismatch there mis-scales the scan interval, and an interval
        // that misses the partition's rows counts zero survivors and wipes the whole (fully live) partition
        // via an empty REPLACE_RANGE.
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, v double, ts timestamp_ns) timestamp(ts) partition by day wal");
            execute("insert into base values " +
                    "('A', 1.0, '2024-01-01T00:00:00.000000000Z')," +
                    "('B', 5.0, '2024-01-02T00:00:00.000000000Z')," +
                    "('C', 9.0, '2024-01-03T00:00:00.000000000Z')");
            drainWalAndMatViewQueues();
            execute("create materialized view mv as (select * from base) expire rows when v < 0.0");
            drainWalAndMatViewQueues();
            assertQuery("select count() p, sum(numRows) r from table_partitions('mv')").noRandomAccess().expectSize().noLeakCheck().returns("p\tr\n3\t3\n");

            final TableToken token = engine.verifyTableName("mv");
            final String predicate;
            try (TableMetadata m = engine.getTableMetadata(token)) {
                predicate = m.getExpiryPredicate();
            }
            try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
                job.cleanupTable(token, predicate);
            }
            drainWalAndMatViewQueues();

            assertQuery("select count() p, sum(numRows) r from table_partitions('mv')").noRandomAccess().expectSize().noLeakCheck().returns("p\tr\n3\t3\n");
            assertQuery("select sym, v from mv order by sym").noLeakCheck().returns("""
                    sym\tv
                    A\t1.0
                    B\t5.0
                    C\t9.0
                    """);
        });
    }

    @Test
    public void testExpireScalarCleanupReclaimsOldPartition() throws Exception {
        // The physical cleanup reclaims on a mat view via REPLACE_RANGE (DROP PARTITION via SQL is rejected
        // for mat views). Here a wholly-below-threshold partition is wiped.
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("""
                    insert into base values
                    ('A', 1.0, '2024-01-01T00:00:00.000000Z'),
                    ('B', 2.0, '2024-01-02T00:00:00.000000Z'),
                    ('C', 3.0, '2024-01-03T00:00:00.000000Z')""");
            drainWalAndMatViewQueues();
            execute("create materialized view mv as (select * from base) expire rows when ts < '2024-01-02T00:00:00.000000Z'");
            drainWalAndMatViewQueues();

            assertQuery("select count() p from table_partitions('mv')").noRandomAccess().expectSize().noLeakCheck().returns("p\n3\n");

            final TableToken token = engine.verifyTableName("mv");
            final String predicate;
            try (TableMetadata m = engine.getTableMetadata(token)) {
                predicate = m.getExpiryPredicate();
            }
            try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
                job.cleanupTable(token, predicate);
            }
            drainWalAndMatViewQueues();

            // 01-01 lies wholly below the threshold -> reclaimed; 01-02 (active-protected check aside) and
            // 01-03 retained. The read filter already hid A; now its storage is gone too.
            assertQuery("select count() p from table_partitions('mv')").noRandomAccess().expectSize().noLeakCheck().returns("p\n2\n");
            assertQuery("select sym, v from mv order by sym").noLeakCheck().returns("""
                    sym\tv
                    B\t2.0
                    C\t3.0
                    """);
        });
    }

    @Test
    public void testExpireScalarKeepsNullPredicateRows() throws Exception {
        // A row whose predicate evaluates to NULL/UNKNOWN (here a NULL v under "v < 2.0") is KEPT, not
        // expired: the read filter is CASE WHEN (pred) THEN false ELSE true, so FALSE and NULL both keep.
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into base values " +
                    "('A', 1.0, '2024-01-05T00:00:00.000000Z')," +   // v < 2 -> expired
                    "('B', 5.0, '2024-01-06T00:00:00.000000Z')," +   // v >= 2 -> kept
                    "('C', null, '2024-01-07T00:00:00.000000Z')");   // v NULL -> kept (UNKNOWN predicate)
            drainWalAndMatViewQueues();
            execute("create materialized view mv as (select * from base) expire rows when v < 2.0");
            drainWalAndMatViewQueues();
            assertQuery("select sym, v from mv order by sym").noLeakCheck().returns("""
                    sym\tv
                    B\t5.0
                    C\tnull
                    """);
        });
    }

    @Test
    public void testExpireScalarCleanupIsIdempotent() throws Exception {
        // A second cleanup sweep over already-compacted data must be a no-op (no REPLACE, partitions
        // unchanged): otherwise it would re-churn the WAL and re-replicate deletions on every sweep.
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into base values " +
                    "('A', 1.0, '2024-01-01T00:00:00.000000Z')," +   // expired (d1 partial)
                    "('B', 5.0, '2024-01-01T00:00:00.000000Z')," +   // kept
                    "('C', 1.5, '2024-01-02T00:00:00.000000Z')," +   // d2 fully expired
                    "('D', 9.0, '2024-01-03T00:00:00.000000Z')");    // active partition
            drainWalAndMatViewQueues();
            execute("create materialized view mv as (select * from base) expire rows when v < 2.0");
            drainWalAndMatViewQueues();

            final TableToken token = engine.verifyTableName("mv");
            final String predicate;
            try (TableMetadata m = engine.getTableMetadata(token)) {
                predicate = m.getExpiryPredicate();
            }
            final boolean first;
            try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
                first = job.cleanupTable(token, predicate);
            }
            drainWalAndMatViewQueues();
            assertTrue("first sweep should reclaim", first);
            assertQuery("select count() p, sum(numRows) r from table_partitions('mv')").noRandomAccess().expectSize().noLeakCheck().returns("p\tr\n2\t2\n");

            // Second sweep: nothing expired remains -> no work, partitions unchanged.
            final boolean second;
            try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
                second = job.cleanupTable(token, predicate);
            }
            drainWalAndMatViewQueues();
            assertFalse("second sweep must be a no-op", second);
            assertQuery("select count() p, sum(numRows) r from table_partitions('mv')").noRandomAccess().expectSize().noLeakCheck().returns("p\tr\n2\t2\n");
        });
    }

    @Test
    public void testExpireScalarLessEqualThresholdCleanup() throws Exception {
        // "<=" exercises the inclusive bounds fast-path: a partition whose nextFloor <= T is wiped without a
        // scan (01-01), and a boundary partition whose rows are exactly == T is expired too (01-02).
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("""
                    insert into base values
                    ('A', 1.0, '2024-01-01T00:00:00.000000Z'),
                    ('B', 2.0, '2024-01-02T00:00:00.000000Z'),
                    ('C', 3.0, '2024-01-03T00:00:00.000000Z')""");
            drainWalAndMatViewQueues();
            execute("create materialized view mv as (select * from base) expire rows when ts <= '2024-01-02T00:00:00.000000Z'");
            drainWalAndMatViewQueues();
            assertQuery("select count() p from table_partitions('mv')").noRandomAccess().expectSize().noLeakCheck().returns("p\n3\n");

            final TableToken token = engine.verifyTableName("mv");
            final String predicate;
            try (TableMetadata m = engine.getTableMetadata(token)) {
                predicate = m.getExpiryPredicate();
            }
            try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
                job.cleanupTable(token, predicate);
            }
            drainWalAndMatViewQueues();

            // 01-01 (nextFloor <= T) wiped by bounds; 01-02 (ts == T, ts <= T) expired by the scan; 01-03 is
            // the active partition and is retained. Only C remains.
            assertQuery("select count() p from table_partitions('mv')").noRandomAccess().expectSize().noLeakCheck().returns("p\n1\n");
            assertQuery("select sym, v from mv order by sym").noLeakCheck().returns("sym\tv\nC\t3.0\n");
        });
    }

    @Test
    public void testAlterMatViewDropExpire() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("""
                    insert into base values
                    ('AAA', 1.0, '2024-01-05T00:00:00.000000Z'),
                    ('BBB', 5.0, '2024-01-09T12:00:00.000000Z')""");
            drainWalAndMatViewQueues();

            // Passthrough view, no policy yet.
            execute("create materialized view mv as (select * from base)");
            drainWalAndMatViewQueues();

            // Set a policy: hide v < 2 -> only BBB visible.
            execute("alter materialized view mv set expire rows when v < 2.0");
            drainWalAndMatViewQueues();
            assertQuery("select sym, v from mv order by sym").noLeakCheck().returns("""
                    sym\tv
                    BBB\t5.0
                    """);

            // Drop the policy: all rows visible again.
            execute("alter materialized view mv drop expire");
            drainWalAndMatViewQueues();

            final TableToken token = engine.verifyTableName("mv");
            try (TableMetadata metadata = engine.getTableMetadata(token)) {
                assertNull(metadata.getExpiryPredicate());
                assertEquals(0, metadata.getExpiryCleanupIntervalMicros());
            }
            assertQuery("select sym, v from mv order by sym").expectSize().noLeakCheck().returns("""
                    sym\tv
                    AAA\t1.0
                    BBB\t5.0
                    """);
        });
    }

    @Test
    public void testShowCreateMatViewWithExpire() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            drainWalAndMatViewQueues();
            execute("create materialized view mv as (select * from base) EXPIRE ROWS WHEN v < 2.0 CLEANUP EVERY 30m");
            drainWalAndMatViewQueues();
            sink.clear();
            printSql("SHOW CREATE MATERIALIZED VIEW mv", sink);
            final String ddl = sink.toString();
            org.junit.Assert.assertTrue("expected EXPIRE clause in: " + ddl, ddl.contains("EXPIRE ROWS WHEN v < 2.0"));
            org.junit.Assert.assertTrue("expected CLEANUP EVERY in: " + ddl, ddl.contains("CLEANUP EVERY 30m"));
        });
    }

    @Test
    public void testAlterMatViewSetExpire() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into base values " +
                    "('AAA', 1.0, '2024-01-05T00:00:00.000000Z')," + // v < 2 -> hidden
                    "('BBB', 2.5, '2024-01-09T12:00:00.000000Z')," + // v >= 2 -> visible
                    "('CCC', 0.5, '2024-01-09T18:00:00.000000Z')");  // v < 2 -> hidden
            drainWalAndMatViewQueues();

            // Passthrough view with no policy at creation.
            execute("create materialized view mv as (select * from base)");
            drainWalAndMatViewQueues();

            execute("alter materialized view mv set expire rows when v < 2.0 cleanup every 30m");
            drainWalAndMatViewQueues();

            // Metadata predicate + cleanup interval are persisted.
            final TableToken token = engine.verifyTableName("mv");
            try (TableMetadata metadata = engine.getTableMetadata(token)) {
                assertEquals("v < 2.0", metadata.getExpiryPredicate());
                assertEquals(30 * 60_000_000L, metadata.getExpiryCleanupIntervalMicros());
            }

            // The read-time filter (reading the policy from the metadata cache) hides v<2 rows.
            assertQuery("select sym, v from mv order by sym").noLeakCheck().returns("""
                    sym\tv
                    BBB\t2.5
                    """);
        });
    }

    @Test
    public void testCreatePassthroughMatViewMirrorsHourlyBasePartitioning() throws Exception {
        // A passthrough view with no explicit PARTITION BY mirrors the base table's partitioning (here HOUR),
        // so refresh REPLACE_RANGE and expiry DROP/REPLACE align to base partitions. Exercises the non-DAY
        // passthrough chunk-interval path.
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by hour wal");
            execute("insert into base values ('AAA', 1.0, '2024-01-05T00:30:00.000000Z')");
            drainWalAndMatViewQueues();
            execute("create materialized view mv as (select * from base) expire rows when v < 2.0");
            drainWalAndMatViewQueues();
            try (TableMetadata metadata = engine.getTableMetadata(engine.verifyTableName("mv"))) {
                assertEquals(PartitionBy.HOUR, metadata.getPartitionBy());
                assertEquals("v < 2.0", metadata.getExpiryPredicate());
            }
        });
    }

    @Test
    public void testCreatePassthroughMatViewWithExpire() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into base values " +
                    "('AAA', 1.0, '2024-01-05T00:00:00.000000Z')," + // v < 2 -> hidden
                    "('BBB', 2.5, '2024-01-09T12:00:00.000000Z')," + // v >= 2 -> visible
                    "('CCC', 0.5, '2024-01-09T18:00:00.000000Z')");  // v < 2 -> hidden
            drainWalAndMatViewQueues();

            // Passthrough (no SAMPLE BY) view that carries the policy from creation.
            execute("create materialized view mv2 as (select * from base) EXPIRE ROWS WHEN v < 2.0");
            drainWalAndMatViewQueues();

            // Predicate persisted; CLEANUP EVERY omitted -> default 1 hour.
            final TableToken token = engine.verifyTableName("mv2");
            try (TableMetadata metadata = engine.getTableMetadata(token)) {
                assertEquals("v < 2.0", metadata.getExpiryPredicate());
                assertEquals(3_600_000_000L, metadata.getExpiryCleanupIntervalMicros());
            }

            // Rows with v < 2 are hidden by the read-time filter.
            assertQuery("select sym, v from mv2 order by sym").noLeakCheck().returns("""
                    sym\tv
                    BBB\t2.5
                    """);
        });
    }

    @Test
    public void testCreateMatViewInvalidPredicateRejected() throws Exception {
        // An invalid EXPIRE predicate must be rejected at CREATE MATERIALIZED VIEW (not accepted and then
        // bricking every read of the view). The view must not be left behind.
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            assertExceptionNoLeakCheck(
                    "create materialized view mv as (select * from base) EXPIRE ROWS WHEN no_such_col < now()",
                    25,
                    "invalid EXPIRE ROWS predicate"
            );
            org.junit.Assert.assertNull(engine.getTableTokenIfExists("mv"));
        });
    }

    @Test
    public void testCreateMatViewNonBooleanPredicateRejected() throws Exception {
        // EXPIRE ROWS WHEN requires a boolean expression; a bare numeric column is rejected at CREATE and
        // the view is not left behind.
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            assertExceptionNoLeakCheck(
                    "create materialized view mv as (select * from base) EXPIRE ROWS WHEN v",
                    25,
                    "invalid EXPIRE ROWS predicate: expected a boolean expression"
            );
            org.junit.Assert.assertNull(engine.getTableTokenIfExists("mv"));
        });
    }

    @Test
    public void testCreateAggregatingMatViewWithExpireAllowed() throws Exception {
        // EXPIRE ROWS on an aggregating (SAMPLE BY) view is allowed but advisory: the cleanup job's physical
        // reclamation is best-effort since a later refresh can regenerate reclaimed rows from surviving base
        // rows, but reads stay correct regardless (the read filter is authoritative), so CREATE succeeds.
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, price double, ts timestamp) timestamp(ts) partition by day wal");
            execute(
                    "create materialized view mv as (" +
                            "select sym, last(price) price, ts from base sample by 1h" +
                            ") partition by day EXPIRE ROWS WHEN ts < dateadd('d', -1, now())"
            );
            drainWalAndMatViewQueues();
            try (TableMetadata metadata = engine.getTableMetadata(engine.verifyTableName("mv"))) {
                org.junit.Assert.assertNotNull(metadata.getExpiryPredicate());
            }
        });
    }

    @Test
    public void testAlterAggregatingMatViewSetExpireAllowed() throws Exception {
        // The same advisory-not-rejected rule applies to ALTER ... SET EXPIRE: an existing aggregating
        // (SAMPLE BY) view may have a policy attached after the fact.
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, price double, ts timestamp) timestamp(ts) partition by day wal");
            execute("create materialized view agg as (select sym, last(price) price, ts from base sample by 1h) partition by day");
            drainWalAndMatViewQueues();
            execute("alter materialized view agg set expire rows when ts < dateadd('d', -1, now())");
            drainWalAndMatViewQueues();
            // The view must be left with the policy attached.
            try (TableMetadata metadata = engine.getTableMetadata(engine.verifyTableName("agg"))) {
                org.junit.Assert.assertNotNull(metadata.getExpiryPredicate());
            }
        });
    }

    @Test
    public void testCreateTableLikeMatViewDoesNotInheritExpire() throws Exception {
        // CREATE TABLE (LIKE <mat view with EXPIRE ROWS>) must NOT copy the policy onto the new PLAIN table:
        // EXPIRE is mat-view-only, and a policy on a plain table would silently hide + physically delete rows.
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("create materialized view mv as (select * from base) EXPIRE ROWS WHEN v < 2.0");
            drainWalAndMatViewQueues();

            // Sanity: the source view does carry the policy.
            try (TableMetadata metadata = engine.getTableMetadata(engine.verifyTableName("mv"))) {
                assertEquals("v < 2.0", metadata.getExpiryPredicate());
            }

            execute("create table cloned (like mv)");
            try (TableMetadata metadata = engine.getTableMetadata(engine.verifyTableName("cloned"))) {
                org.junit.Assert.assertNull(metadata.getExpiryPredicate());
                assertEquals(0L, metadata.getExpiryCleanupIntervalMicros());
            }

            // A v < 2 row in the clone must remain visible -- the leaked read filter would have hidden it.
            execute("insert into cloned values ('AAA', 1.0, '2024-01-05T00:00:00.000000Z')");
            drainWalQueue();
            assertQuery("select sym, v from cloned").expectSize().noLeakCheck().returns("sym\tv\nAAA\t1.0\n");
        });
    }

    @Test
    public void testCreateKeepColumnCollisionRejected() throws Exception {
        // The window/keep-by read filter projects a synthetic boolean column __qdb_re_keep. A view that already
        // exposes that name (here inherited from the base via select *) would make every read ambiguous, so the
        // policy is rejected at CREATE.
        assertMemoryLeak(() -> {
            execute("create table base (__qdb_re_keep int, v double, k symbol, ts timestamp) timestamp(ts) partition by day wal");
            assertExceptionNoLeakCheck(
                    "create materialized view mv as (select * from base) expire rows keep highest v partition by k",
                    25,
                    "cannot be used on a view with a column named '__qdb_re_keep'"
            );
            org.junit.Assert.assertNull(engine.getTableTokenIfExists("mv"));
        });
    }

    @Test
    public void testAlterKeepColumnCollisionRejected() throws Exception {
        // Same __qdb_re_keep collision guard on the ALTER ... SET EXPIRE path (error points at the keep clause).
        assertMemoryLeak(() -> {
            execute("create table base (__qdb_re_keep int, v double, k symbol, ts timestamp) timestamp(ts) partition by day wal");
            execute("create materialized view mv as (select * from base)");
            drainWalAndMatViewQueues();
            assertExceptionNoLeakCheck(
                    "alter materialized view mv set expire rows keep highest v partition by k",
                    43,
                    "cannot be used on a view with a column named '__qdb_re_keep'"
            );
            try (TableMetadata metadata = engine.getTableMetadata(engine.verifyTableName("mv"))) {
                org.junit.Assert.assertNull(metadata.getExpiryPredicate());
            }
        });
    }

    @Test
    public void testReadFilterCorrectForNonMonotonicFuturePredicate() throws Exception {
        // ts > now() is NON-MONOTONIC: a future-dated row is hidden now but must REAPPEAR once now() advances
        // past its timestamp. The read filter recomputes now() on every read, so it stays correct regardless.
        // (Physical cleanup is documented as unsafe for such predicates -- this locks in the read-side invariant
        // that the row is only hidden, never logically gone.)
        assertMemoryLeak(() -> {
            setCurrentMicros(1_704_844_800_000_000L); // 2024-01-10T00:00:00Z
            execute("create table base (sym symbol, ts timestamp) timestamp(ts) partition by day wal");
            execute("create materialized view mv as (select * from base) expire rows when ts > now()");
            execute("insert into base values " +
                    "('PAST', '2024-01-05T00:00:00.000000Z')," +   // < now -> kept
                    "('FUTURE', '2024-01-20T00:00:00.000000Z')");  // > now -> hidden
            drainWalAndMatViewQueues();

            // At 2024-01-10 only the past row is visible.
            assertQuery("select sym from mv order by ts").noLeakCheck().returns("sym\nPAST\n");

            // Advance now() beyond the future row's ts: it reappears (it was only hidden, never deleted).
            setCurrentMicros(1_706_140_800_000_000L); // 2024-01-25T00:00:00Z
            assertQuery("select sym from mv order by ts").noLeakCheck().returns("sym\nPAST\nFUTURE\n");
        });
    }

    @Test
    public void testReadFilterTimestampNullConstantKeepsAllRows() throws Exception {
        // ts < cast(null as timestamp) is never TRUE, so NO row expires -- all rows stay visible. The
        // null-unsafe flip (NOT(ts < T) -> ts >= T) would have produced `ts >= NULL` and hidden EVERY row
        // (read/cleanup divergence: cleanup keeps them). The provably-non-null guard keeps the CASE form here.
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, ts timestamp) timestamp(ts) partition by day wal");
            execute("create materialized view mv as (select * from base) expire rows when ts < cast(null as timestamp)");
            execute("""
                    insert into base values
                    ('A', '2024-01-05T00:00:00.000000Z'),
                    ('B', '2024-01-20T00:00:00.000000Z')""");
            drainWalAndMatViewQueues();
            assertQuery("select sym from mv order by ts").noLeakCheck().returns("sym\nA\nB\n");
        });
    }

    @Test
    public void testApplyTimeGuardIgnoresExpireOnPlainTable() throws Exception {
        // Defense-in-depth (apply/write side): setMetaExpiry must NOT persist a policy onto a non-mat-view,
        // even if a malformed/forged alter reaches the writer (the SQL compiler already rejects this; here we
        // bypass it by calling the writer directly). It logs + skips rather than throwing -- a throw on WAL
        // apply would suspend the table.
        assertMemoryLeak(() -> {
            execute("create table t (a int, ts timestamp) timestamp(ts) partition by day bypass wal");
            final TableToken token = engine.verifyTableName("t");
            try (TableWriter writer = getWriter("t")) {
                writer.setMetaExpiry("a < 2", 3_600_000_000L);
            }
            try (TableMetadata metadata = engine.getTableMetadata(token)) {
                org.junit.Assert.assertNull("policy must not persist on a plain table", metadata.getExpiryPredicate());
            }
        });
    }

    @Test
    public void testCreateDistinctViewWithExpireRejected() throws Exception {
        // A DISTINCT defining query deduplicates base rows, so it is NOT a 1:1 passthrough and must not carry
        // EXPIRE ROWS (physical cleanup could delete a row the view still needs). isPassthrough rejects it
        // via the group-by model that DISTINCT lowers to, before EXPIRE is evaluated.
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            assertExceptionNoLeakCheck(
                    "create materialized view mvds as (select distinct sym, v, ts from base) EXPIRE ROWS WHEN v < 2.0",
                    34,
                    "TIMESTAMP column is not present in select list"
            );
            org.junit.Assert.assertNull(engine.getTableTokenIfExists("mvds"));
        });
    }

    @Test
    public void testCreateDistinctViewWithTimestampWrapperRejected() throws Exception {
        // Regression: a DISTINCT sub-query wrapped so a timestamp(ts) designation survives to the top must
        // still be rejected. isSqlDistinctGroupByRewriteEnabled() rewrites DISTINCT into an implicit
        // SELECT_MODEL_GROUP_BY whose getGroupBy() list is empty and whose isDistinct() flag is cleared, so
        // isPassthrough must reject the group-by MODEL, not just an explicit GROUP BY clause. Before the fix
        // this DISTINCT-derived (non-1:1) view was created as a passthrough and would receive EXPIRE ROWS
        // physical cleanup.
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            assertExceptionNoLeakCheck(
                    "create materialized view mvdw as (select * from (select distinct sym, v, ts from base) timestamp(ts)) EXPIRE ROWS WHEN v < 2.0",
                    34,
                    "TIMESTAMP column is not present in select list"
            );
            org.junit.Assert.assertNull(engine.getTableTokenIfExists("mvdw"));
        });
    }

    @Test
    public void testCreateGroupByViewWithExpireRejected() throws Exception {
        // A GROUP BY nested under the defining query collapses base rows, so it is NOT a 1:1 passthrough. The
        // nested-chain passthrough check must reject it: the top-level isNotPlainSelectModel() check does not
        // see a GROUP BY that lowered onto a nested model, so without the nested-chain GROUP BY arm the view
        // would be created as a passthrough and EXPIRE ROWS physical cleanup would run on a derived view. The
        // outer timestamp(ts) keeps a designated timestamp so the earlier "requires designated timestamp"
        // check does not mask the passthrough classification. Regression guard for the nested GROUP BY arm.
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            assertExceptionNoLeakCheck(
                    "create materialized view mvgb as (select * from (select sym, v, ts from base group by sym, v, ts) timestamp(ts)) EXPIRE ROWS WHEN v < 2.0",
                    34,
                    "TIMESTAMP column is not present in select list"
            );
            org.junit.Assert.assertNull(engine.getTableTokenIfExists("mvgb"));
        });
    }

    @Test
    public void testCreateLatestOnViewWithExpireRejected() throws Exception {
        // A LATEST ON defining query is NOT a 1:1 passthrough -- its rows are a per-key reduction of the base
        // -- so it must not be classified as a passthrough mat view, otherwise EXPIRE ROWS' physical cleanup
        // would delete rows from a derived view. (LATEST ON lowers onto a NESTED model, which the prior
        // top-model-only passthrough check missed.) It is rejected as a non-passthrough/non-aggregating mat
        // view before EXPIRE is evaluated. Regression guard for the passthrough-classification fix.
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            assertExceptionNoLeakCheck(
                    "create materialized view mvlo as (select * from base latest on ts partition by sym) EXPIRE ROWS WHEN v < 2.0",
                    34,
                    "TIMESTAMP column is not present in select list"
            );
            org.junit.Assert.assertNull(engine.getTableTokenIfExists("mvlo"));
        });
    }

    @Test
    public void testCreateUnionViewWithExpireRejected() throws Exception {
        // A UNION nested under the defining query is NOT a 1:1 passthrough (it can multiply rows). The
        // nested-chain passthrough check must reject it: the top-level isNotPlainSelectModel() check does not
        // see a UNION that lowered onto a nested model, so without the nested-chain UNION arm the view would
        // be created as a passthrough and EXPIRE ROWS physical cleanup would run on a derived view. The outer
        // timestamp(ts) keeps a designated timestamp so the earlier "requires designated timestamp" check does
        // not mask the passthrough classification. Regression guard for the nested UNION arm of isPassthrough.
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            assertExceptionNoLeakCheck(
                    "create materialized view mvun as (select * from (select * from base union all select * from base) timestamp(ts)) EXPIRE ROWS WHEN v < 2.0",
                    34,
                    "TIMESTAMP column is not present in select list"
            );
            org.junit.Assert.assertNull(engine.getTableTokenIfExists("mvun"));
        });
    }

    @Test
    public void testMatViewExpirePersistsAfterReopen() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("create materialized view mv as (select * from base) EXPIRE ROWS WHEN v < 2.0 cleanup every 15m");
            drainWalAndMatViewQueues();

            final TableToken token = engine.verifyTableName("mv");

            // Drop pooled readers/writers and re-read the policy from disk (_meta).
            engine.releaseInactive();
            try (TableMetadata metadata = engine.getTableMetadata(token)) {
                assertEquals("v < 2.0", metadata.getExpiryPredicate());
                assertEquals(15 * 60_000_000L, metadata.getExpiryCleanupIntervalMicros());
            }
        });
    }

    @Test
    public void testPoliciedMatViewInJoinHidesExpiredRows() throws Exception {
        // A policied mat view referenced inside a JOIN must still have the read filter applied on its side,
        // so expired view rows are hidden from the join result.
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into base values " +
                    "('AAA', 1.0, '2024-01-05T00:00:00.000000Z')," + // v<2 -> expired in mv
                    "('BBB', 5.0, '2024-01-06T00:00:00.000000Z')");  // v>=2 -> live in mv
            drainWalAndMatViewQueues();
            execute("create materialized view mv as (select * from base) EXPIRE ROWS WHEN v < 2.0");
            drainWalAndMatViewQueues();

            execute("create table dim (sym symbol, label string)");
            execute("""
                    insert into dim values
                    ('AAA', 'a'),
                    ('BBB', 'b')""");

            // Only BBB survives in mv (v>=2), so the join yields only BBB even though dim has AAA.
            assertQuery("select mv.sym, mv.v, dim.label from mv join dim on mv.sym = dim.sym order by mv.sym").noLeakCheck().returns("""
                    sym\tv\tlabel
                    BBB\t5.0\tb
                    """);
        });
    }

    @Test
    public void testCreateExpireRowsUnbalancedOpenParenRejected() throws Exception {
        // M4: an open paren that is never closed must be detected, not silently swallow the trailing
        // CLEANUP clause into the predicate text (which would also drop the custom cleanup interval).
        // The error is reported at the start of the predicate.
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            assertExceptionNoLeakCheck(
                    "create materialized view mv as (select * from base) EXPIRE ROWS WHEN (x > 1 CLEANUP EVERY 1h",
                    69,
                    "unbalanced parentheses in EXPIRE ROWS predicate"
            );
            org.junit.Assert.assertNull(engine.getTableTokenIfExists("mv"));
        });
    }

    @Test
    public void testCreateExpireRowsUnbalancedCloseParenRejected() throws Exception {
        // M4: a ')' with no matching '(' (depth would go negative) must be flagged at the offending ')'.
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            assertExceptionNoLeakCheck(
                    "create materialized view mv as (select * from base) EXPIRE ROWS WHEN x > 1) AND y",
                    74,
                    "unbalanced parentheses in EXPIRE ROWS predicate"
            );
            org.junit.Assert.assertNull(engine.getTableTokenIfExists("mv"));
        });
    }

    @Test
    public void testCreateExpireRowsBalancedParensWithCleanupParses() throws Exception {
        // M4 regression guard: a fully-balanced parenthesised predicate followed by CLEANUP EVERY must
        // still parse, with CLEANUP terminating the predicate (not swallowed) and the custom interval kept.
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("create materialized view mv as (select * from base) EXPIRE ROWS WHEN (v < 2.0) CLEANUP EVERY 30m");
            sink.clear();
            printSql("SHOW CREATE MATERIALIZED VIEW mv", sink);
            final String ddl = sink.toString();
            org.junit.Assert.assertTrue("expected balanced predicate in: " + ddl, ddl.contains("EXPIRE ROWS WHEN (v < 2.0)") || ddl.contains("EXPIRE ROWS WHEN v < 2.0"));
            org.junit.Assert.assertTrue("expected custom CLEANUP interval in: " + ddl, ddl.contains("CLEANUP EVERY 30m"));
        });
    }

    @Test
    public void testCreateExpireRowsColumnNamedCleanupParses() throws Exception {
        // NIT: a column reference literally named "cleanup" must be treated as predicate content, not as
        // the CLEANUP clause boundary. CLEANUP is a boundary only when immediately followed by EVERY.
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, cleanup double, ts timestamp) timestamp(ts) partition by day wal");
            execute("create materialized view mv as (select * from base) EXPIRE ROWS WHEN cleanup > 5");
            final String predicate;
            try (TableMetadata m = engine.getTableMetadata(engine.verifyTableName("mv"))) {
                predicate = m.getExpiryPredicate();
            }
            org.junit.Assert.assertNotNull(predicate);
            org.junit.Assert.assertTrue("predicate should reference the cleanup column: " + predicate, predicate.contains("cleanup"));
        });
    }

    @Test
    public void testCreateExpireRowsNonParenSelectRejected() throws Exception {
        // #4 (decision: not supported): EXPIRE ROWS is only reachable when the SELECT is parenthesised.
        // In the non-paren AS form the SELECT parser greedily consumes EXPIRE as a table alias for "base"
        // and then reports the following token (ROWS) as unexpected. The statement is still rejected (the
        // view is not created); supporting the bare form would require the DML parser to stop at EXPIRE.
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            assertExceptionNoLeakCheck(
                    "create materialized view mv as select * from base EXPIRE ROWS WHEN v < 2.0",
                    57,
                    "unexpected token [ROWS]"
            );
            org.junit.Assert.assertNull(engine.getTableTokenIfExists("mv"));
        });
    }

    @Test
    public void testAlterMatViewDropExpireUnexpectedTokenRejected() throws Exception {
        // DROP EXPIRE [ROWS] accepts nothing else after it; a trailing token is a clear syntax error, not
        // silently ignored (which could mask a typo intended as a different clause).
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("create materialized view mv as (select * from base) expire rows when v < 2.0");
            drainWalAndMatViewQueues();
            assertExceptionNoLeakCheck(
                    "alter materialized view mv drop expire foo",
                    39,
                    "unexpected token [foo] while trying to drop row-expiry policy"
            );
            // The policy is untouched.
            try (TableMetadata metadata = engine.getTableMetadata(engine.verifyTableName("mv"))) {
                org.junit.Assert.assertNotNull(metadata.getExpiryPredicate());
            }
        });
    }

    @Test
    public void testAlterMatViewDropNonExpireRejected() throws Exception {
        // ALTER MATERIALIZED VIEW ... DROP is only valid as DROP EXPIRE; any other continuation (or none)
        // must report "'expire' expected".
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("create materialized view mv as (select * from base)");
            drainWalAndMatViewQueues();
            assertExceptionNoLeakCheck(
                    "alter materialized view mv drop ttl",
                    32,
                    "'expire' expected"
            );
            assertExceptionNoLeakCheck(
                    "alter materialized view mv drop",
                    31,
                    "'expire' expected"
            );
        });
    }

    @Test
    public void testAlterMatViewSetExpireInvalidPredicateRejected() throws Exception {
        // ALTER ... SET EXPIRE validates the predicate by probing it against the (existing) view; an unknown
        // column surfaces as "invalid EXPIRE ROWS predicate" and the policy is not applied.
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("create materialized view mv as (select * from base)");
            drainWalAndMatViewQueues();
            assertExceptionNoLeakCheck(
                    "alter materialized view mv set expire rows when no_such < 2.0",
                    48,
                    "invalid EXPIRE ROWS predicate"
            );
            try (TableMetadata metadata = engine.getTableMetadata(engine.verifyTableName("mv"))) {
                org.junit.Assert.assertNull(metadata.getExpiryPredicate());
            }
        });
    }

    @Test
    public void testAlterMatViewSetExpireNonBooleanPredicateRejected() throws Exception {
        // EXPIRE ROWS WHEN requires a boolean expression; a bare numeric column is rejected at ALTER and
        // the view keeps no policy.
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("create materialized view mv as (select * from base)");
            drainWalAndMatViewQueues();
            assertExceptionNoLeakCheck(
                    "alter materialized view mv set expire rows when v",
                    48,
                    "invalid EXPIRE ROWS predicate: expected a boolean expression"
            );
            try (TableMetadata metadata = engine.getTableMetadata(engine.verifyTableName("mv"))) {
                org.junit.Assert.assertNull(metadata.getExpiryPredicate());
            }
        });
    }

    @Test
    public void testReadFilterMemoizesFlipEligibilityOnCairoTable() throws Exception {
        // The parser derives the keep-filter's flip verdict once per CairoTable instance; every later
        // compile reads the memo. Proven by planting the opposite verdict on the live instance: a fresh
        // compile (distinct SQL text, so the query cache cannot serve it) obeys the planted value. A
        // policy or metadata change replaces the CairoTable, so a stale memo cannot survive a change.
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("create materialized view mv as (select * from base) expire rows when ts < '2024-01-02T00:00:00.000000Z'");
            drainWalAndMatViewQueues();

            final CairoTable table;
            try (MetadataCacheReader ro = engine.getMetadataCache().readLock()) {
                table = ro.getTable(engine.verifyTableName("mv"));
            }
            assertNotNull(table);
            assertEquals(CairoTable.EXPIRY_FLIP_UNKNOWN, table.getExpiryFlipEligibility());

            // A DECLARE-carrying compile neither trusts nor populates the memo: a declared name can capture
            // an unquoted '@'-prefixed column reference in the predicate during the probe parse.
            printSql("explain declare @unused := 1 select * from mv");
            TestUtils.assertContains(sink, "Interval forward scan");
            assertEquals(CairoTable.EXPIRY_FLIP_UNKNOWN, table.getExpiryFlipEligibility());

            printSql("explain select * from mv");
            TestUtils.assertContains(sink, "Interval forward scan");
            assertEquals(CairoTable.EXPIRY_FLIP_YES, table.getExpiryFlipEligibility());

            table.setExpiryFlipEligibility(CairoTable.EXPIRY_FLIP_NO);
            printSql("explain select * from mv where true");
            TestUtils.assertContains(sink, "case(");

            table.setExpiryFlipEligibility(CairoTable.EXPIRY_FLIP_YES);
            printSql("explain select * from mv where not false");
            TestUtils.assertContains(sink, "Interval forward scan");
        });
    }

    @Test
    public void testReadFilterMemoizesWindowColumnListOnCairoTable() throws Exception {
        // The window read-filter's outer projection CSV is derived once per CairoTable instance; every
        // later compile reads the memo. Proven by planting a narrower projection: a fresh compile serves
        // only the planted columns.
        assertMemoryLeak(() -> {
            execute("create table base (k symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("create materialized view mv as (select * from base) expire rows keep highest v partition by k");
            drainWalAndMatViewQueues();

            printSql("select * from mv");
            TestUtils.assertEquals("k\tv\tts\n", sink);

            final CairoTable table;
            try (MetadataCacheReader ro = engine.getMetadataCache().readLock()) {
                table = ro.getTable(engine.verifyTableName("mv"));
            }
            assertNotNull(table);
            assertEquals("\"k\",\"v\",\"ts\"", table.getExpiryQuotedColumnsCsv());

            table.setExpiryQuotedColumnsCsv("\"k\",\"ts\"");
            printSql("select * from mv where true");
            TestUtils.assertEquals("k\tts\n", sink);

            table.setExpiryQuotedColumnsCsv(null);
            printSql("select * from mv where not false");
            TestUtils.assertEquals("k\tv\tts\n", sink);
        });
    }

    @Test
    public void testPassthroughFlagPersistedInDefinitionFile() throws Exception {
        // The passthrough flag drives refresh REPLACE_RANGE chunking and the SET EXPIRE advisory for
        // aggregating views, so it must survive a definition reload: assert it via the live graph and by
        // re-reading the _mv definition file from disk after cached objects are released.
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("create materialized view mv as (select * from base) expire rows when v < 2.0");
            execute("create materialized view agg as (select sym, last(v) v, ts from base sample by 1h) partition by day");
            drainWalAndMatViewQueues();

            final TableToken mvToken = engine.verifyTableName("mv");
            final TableToken aggToken = engine.verifyTableName("agg");
            assertTrue(engine.getMatViewGraph().getViewDefinition(mvToken).isPassthrough());
            assertFalse(engine.getMatViewGraph().getViewDefinition(aggToken).isPassthrough());

            engine.releaseInactive();

            try (BlockFileReader reader = new BlockFileReader(configuration); Path path = new Path()) {
                path.of(configuration.getDbRoot());
                final int rootLen = path.size();
                final MatViewDefinition reloadedMv = new MatViewDefinition();
                MatViewDefinition.readFrom(engine, reloadedMv, reader, path, rootLen, mvToken);
                assertTrue("passthrough flag must survive the _mv definition round-trip", reloadedMv.isPassthrough());
                final MatViewDefinition reloadedAgg = new MatViewDefinition();
                MatViewDefinition.readFrom(engine, reloadedAgg, reader, path, rootLen, aggToken);
                assertFalse(reloadedAgg.isPassthrough());
            }

            // The live graph still serves the flag after the release.
            assertTrue(engine.getMatViewGraph().getViewDefinition(mvToken).isPassthrough());
        });
    }

    @Test
    public void testAlterMatViewSetExpireTrailingTokenRejected() throws Exception {
        // A well-formed clause followed by a stray token (after CLEANUP EVERY here) is a syntax error, so a
        // typo cannot silently persist a truncated policy.
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("create materialized view mv as (select * from base)");
            drainWalAndMatViewQueues();
            assertExceptionNoLeakCheck(
                    "alter materialized view mv set expire rows when v < 2.0 cleanup every 1h foo",
                    73,
                    "unexpected token [foo] while trying to set row-expiry policy"
            );
            try (TableMetadata metadata = engine.getTableMetadata(engine.verifyTableName("mv"))) {
                org.junit.Assert.assertNull(metadata.getExpiryPredicate());
            }
        });
    }

    @Test
    public void testAlterTableDropExpireRejected() throws Exception {
        // EXPIRE ROWS is materialized-view-only, so ALTER TABLE ... DROP EXPIRE on a plain table must give the
        // specific message rather than a generic one.
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            assertExceptionNoLeakCheck(
                    "alter table base drop expire",
                    22,
                    "EXPIRE ROWS is only supported on materialized views"
            );
        });
    }

    @Test
    public void testAlterTableSetExpireRejected() throws Exception {
        // Same for ALTER TABLE ... SET EXPIRE on a plain table.
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            assertExceptionNoLeakCheck(
                    "alter table base set expire rows when v < 2.0",
                    21,
                    "EXPIRE ROWS is only supported on materialized views"
            );
        });
    }

    @Test
    public void testCleanupIntervalTooLargeRejected() throws Exception {
        // A CLEANUP EVERY multiple that overflows when converted to micros must fail cleanly at parse time
        // rather than persisting a garbage (possibly negative) cadence.
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            assertExceptionNoLeakCheck(
                    "create materialized view mv as (select * from base) expire rows when v < 2.0 cleanup every 999999999w",
                    91,
                    "cleanup interval is too large"
            );
            org.junit.Assert.assertNull(engine.getTableTokenIfExists("mv"));
        });
    }

    @Test
    public void testCleanupIntervalUnsupportedUnitRejected() throws Exception {
        // CLEANUP EVERY accepts s/m/h/d/w; a month/year unit is a valid stride unit elsewhere but not a
        // cleanup cadence, so it is rejected with the specific message.
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            assertExceptionNoLeakCheck(
                    "create materialized view mv as (select * from base) expire rows when v < 2.0 cleanup every 1y",
                    91,
                    "unsupported cleanup interval unit, expected s/m/h/d/w"
            );
            org.junit.Assert.assertNull(engine.getTableTokenIfExists("mv"));
        });
    }

    @Test
    public void testCleanupJobRunSeriallyReclaimsOldPartition() throws Exception {
        // Drive the background job via run() (the discovery sweep) rather than calling cleanupTable directly:
        // runSerially() must find the policied view through the metadata cache and reclaim its wholly-expired
        // non-active partition.
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("""
                    insert into base values
                    ('A', 1.0, '2024-01-01T00:00:00.000000Z'),
                    ('B', 2.0, '2024-01-02T00:00:00.000000Z'),
                    ('C', 3.0, '2024-01-03T00:00:00.000000Z')""");
            drainWalAndMatViewQueues();
            execute("create materialized view mv as (select * from base) expire rows when ts < '2024-01-02T00:00:00.000000Z'");
            drainWalAndMatViewQueues();
            // Read the view so it is hydrated into the metadata cache the discovery sweep scans.
            assertQuery("select count() p from table_partitions('mv')").noRandomAccess().expectSize().noLeakCheck().returns("p\n3\n");

            try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
                assertTrue("discovery sweep should reclaim", job.run(Job.RUNNING_STATUS));
            }
            drainWalAndMatViewQueues();

            assertQuery("select count() p from table_partitions('mv')").noRandomAccess().expectSize().noLeakCheck().returns("p\n2\n");
            assertQuery("select sym, v from mv order by sym").noLeakCheck().returns("""
                    sym\tv
                    B\t2.0
                    C\t3.0
                    """);
        });
    }

    @Test
    public void testEmptyWhenPredicateRejected() throws Exception {
        // WHEN with no predicate text before the next boundary (CLEANUP here) is a syntax error.
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            assertExceptionNoLeakCheck(
                    "create materialized view mv as (select * from base) expire rows when cleanup every 1h",
                    69,
                    "EXPIRE ROWS WHEN predicate is empty"
            );
            org.junit.Assert.assertNull(engine.getTableTokenIfExists("mv"));
        });
    }

    private void assertCachedPlanCompiledDuringPolicyChange(
            String initialExpiryClause,
            String initialExpected,
            String policyChangeSql,
            String expected
    ) throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (k SYMBOL, v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO base VALUES
                    ('A', 1.0, '2024-01-01T00:00:00.000000Z'),
                    ('B', 2.0, '2024-01-02T00:00:00.000000Z'),
                    ('C', 3.0, '2024-01-03T00:00:00.000000Z')
                    """);
            execute("CREATE MATERIALIZED VIEW mv AS (SELECT * FROM base)" + initialExpiryClause);
            drainWalAndMatViewQueues();

            // Warm the cache and prove the policy state from which the ALTER starts.
            if (initialExpiryClause.isEmpty()) {
                assertQuery("SELECT k, v FROM mv ORDER BY k").expectSize().noLeakCheck().returns(initialExpected);
            } else {
                assertQuery("SELECT k, v FROM mv ORDER BY k").noLeakCheck().returns(initialExpected);
            }

            final AtomicReference<Throwable> applyError = new AtomicReference<>();
            final CountDownLatch metadataVersionPublished = new CountDownLatch(1);
            final CountDownLatch resumeCacheHydration = new CountDownLatch(1);
            execute(policyChangeSql);
            TableWriter.setMetadataVersionPublishedBarrier(() -> {
                metadataVersionPublished.countDown();
                try {
                    if (!resumeCacheHydration.await(30, TimeUnit.SECONDS)) {
                        throw new AssertionError("timed out waiting to resume metadata-cache hydration");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(e);
                }
            });

            final Thread applyThread = new Thread(() -> {
                try {
                    drainWalQueue();
                } catch (Throwable th) {
                    applyError.set(th);
                } finally {
                    Path.clearThreadLocals();
                }
            }, "expire-policy-apply");

            try {
                applyThread.start();
                assertTrue(
                        "ALTER did not publish its metadata version",
                        metadataVersionPublished.await(30, TimeUnit.SECONDS)
                );
                // _meta/_txn now expose V+1 while MetadataCache still contains policy P0. The factory must not
                // combine P0 with V+1 and remain valid after hydration publishes the current policy.
                try (RecordCursorFactory factory = select("SELECT k, v FROM mv ORDER BY k")) {
                    resumeCacheHydration.countDown();
                    applyThread.join(30_000);
                    assertFalse("WAL apply did not finish", applyThread.isAlive());
                    if (applyError.get() != null) {
                        throw new AssertionError("WAL apply failed", applyError.get());
                    }
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        assertCursor(expected, cursor, factory.getMetadata(), true);
                    }
                }
            } finally {
                resumeCacheHydration.countDown();
                TableWriter.setMetadataVersionPublishedBarrier(null);
                applyThread.join(30_000);
            }
        });
    }

    private void assertInsertSelectCompiledDuringPolicyChange(
            String initialExpiryClause,
            String policyChangeSql,
            String expected
    ) throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (k SYMBOL, v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO base VALUES
                    ('A', 1.0, '2024-01-01T00:00:00.000000Z'),
                    ('B', 2.0, '2024-01-02T00:00:00.000000Z'),
                    ('C', 3.0, '2024-01-03T00:00:00.000000Z')
                    """);
            execute("CREATE MATERIALIZED VIEW mv AS (SELECT * FROM base)" + initialExpiryClause);
            execute("CREATE TABLE sink (k SYMBOL, v DOUBLE)");
            drainWalAndMatViewQueues();

            // Warm the cache so the INSERT model captures P0 before the transition starts.
            if (initialExpiryClause.isEmpty()) {
                assertQuery("SELECT k, v FROM mv ORDER BY k").expectSize().noLeakCheck().returns(
                        "k\tv\nA\t1.0\nB\t2.0\nC\t3.0\n"
                );
            } else {
                assertQuery("SELECT k, v FROM mv ORDER BY k").noLeakCheck().returns(
                        "k\tv\nB\t2.0\nC\t3.0\n"
                );
            }

            final AtomicInteger factoryGenerationAttempts = new AtomicInteger();
            final AtomicReference<Throwable> applyError = new AtomicReference<>();
            final CountDownLatch insertModelCompiled = new CountDownLatch(1);
            final CountDownLatch metadataVersionPublished = new CountDownLatch(1);
            final CountDownLatch resumeCacheHydration = new CountDownLatch(1);
            execute(policyChangeSql);
            TableWriter.setMetadataVersionPublishedBarrier(() -> {
                metadataVersionPublished.countDown();
                try {
                    if (!resumeCacheHydration.await(30, TimeUnit.SECONDS)) {
                        throw new AssertionError("timed out waiting to resume metadata-cache hydration");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(e);
                }
            });
            SqlCompilerImpl.setInsertSelectFactoryGenerationBarrier(() -> {
                if (factoryGenerationAttempts.getAndIncrement() == 0) {
                    insertModelCompiled.countDown();
                    try {
                        if (!metadataVersionPublished.await(30, TimeUnit.SECONDS)) {
                            throw new AssertionError("timed out waiting for metadata-version publication");
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new AssertionError(e);
                    }
                }
            });

            final Thread applyThread = new Thread(() -> {
                try {
                    if (!insertModelCompiled.await(30, TimeUnit.SECONDS)) {
                        throw new AssertionError("timed out waiting for INSERT SELECT model compilation");
                    }
                    drainWalQueue();
                } catch (Throwable th) {
                    applyError.set(th);
                } finally {
                    Path.clearThreadLocals();
                }
            }, "expire-policy-insert-select-apply");

            try {
                applyThread.start();
                try (
                        SqlCompiler sqlCompiler = engine.getSqlCompiler();
                        InsertOperation insertOperation = sqlCompiler.compile(
                                "INSERT INTO sink SELECT k, v FROM mv ORDER BY k",
                                sqlExecutionContext
                        ).popInsertOperation()
                ) {
                    resumeCacheHydration.countDown();
                    applyThread.join(30_000);
                    assertFalse("WAL apply did not finish", applyThread.isAlive());
                    if (applyError.get() != null) {
                        throw new AssertionError("WAL apply failed", applyError.get());
                    }
                    try (OperationFuture future = insertOperation.execute(sqlExecutionContext)) {
                        future.await();
                    }
                }
                assertEquals("INSERT SELECT must retry after the policy epoch changes", 2, factoryGenerationAttempts.get());
                assertQuery("SELECT k, v FROM sink ORDER BY k").expectSize().noLeakCheck().returns(expected);
            } finally {
                insertModelCompiled.countDown();
                resumeCacheHydration.countDown();
                SqlCompilerImpl.setInsertSelectFactoryGenerationBarrier(null);
                TableWriter.setMetadataVersionPublishedBarrier(null);
                applyThread.join(30_000);
            }
        });
    }

    private void assertViewCompiledDuringPolicyChange(
            String initialExpiryClause,
            String initialViewSql,
            String policyChangeSql,
            String viewSql,
            String expected,
            String expectedPolicyColumn,
            String stalePolicyColumn
    ) throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (k SYMBOL, v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO base VALUES
                    ('A', 1.0, '2024-01-01T00:00:00.000000Z'),
                    ('B', 2.0, '2024-01-02T00:00:00.000000Z'),
                    ('C', 3.0, '2024-01-03T00:00:00.000000Z')
                    """);
            execute("CREATE MATERIALIZED VIEW mv AS (SELECT * FROM base)" + initialExpiryClause);
            drainWalAndMatViewQueues();
            if (initialViewSql != null) {
                execute(initialViewSql);
                drainWalAndViewQueues();
            }

            if (initialExpiryClause.isEmpty()) {
                assertQuery("SELECT k FROM mv ORDER BY k").expectSize().noLeakCheck().returns("k\nA\nB\nC\n");
            } else {
                assertQuery("SELECT k FROM mv ORDER BY k").noLeakCheck().returns("k\nB\nC\n");
            }

            final AtomicInteger factoryGenerationAttempts = new AtomicInteger();
            final AtomicReference<Throwable> applyError = new AtomicReference<>();
            final CountDownLatch metadataVersionPublished = new CountDownLatch(1);
            final CountDownLatch resumeCacheHydration = new CountDownLatch(1);
            final CountDownLatch viewModelCompiled = new CountDownLatch(1);
            execute(policyChangeSql);
            TableWriter.setMetadataVersionPublishedBarrier(() -> {
                metadataVersionPublished.countDown();
                try {
                    if (!resumeCacheHydration.await(30, TimeUnit.SECONDS)) {
                        throw new AssertionError("timed out waiting to resume metadata-cache hydration");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(e);
                }
            });
            SqlCompilerImpl.setViewFactoryGenerationBarrier(() -> {
                if (factoryGenerationAttempts.getAndIncrement() == 0) {
                    viewModelCompiled.countDown();
                    try {
                        if (!metadataVersionPublished.await(30, TimeUnit.SECONDS)) {
                            throw new AssertionError("timed out waiting for metadata-version publication");
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new AssertionError(e);
                    }
                }
            });

            final Thread applyThread = new Thread(() -> {
                try {
                    if (!viewModelCompiled.await(30, TimeUnit.SECONDS)) {
                        throw new AssertionError("timed out waiting for view model compilation");
                    }
                    drainWalQueue();
                } catch (Throwable th) {
                    applyError.set(th);
                } finally {
                    Path.clearThreadLocals();
                }
            }, "expire-policy-view-apply");

            try {
                applyThread.start();
                execute(viewSql);
                resumeCacheHydration.countDown();
                applyThread.join(30_000);
                assertFalse("WAL apply did not finish", applyThread.isAlive());
                if (applyError.get() != null) {
                    throw new AssertionError("WAL apply failed", applyError.get());
                }
                assertEquals("view compilation must retry after the policy epoch changes", 2, factoryGenerationAttempts.get());
                final TableToken viewToken = engine.getTableTokenIfExists("v1");
                assertNotNull(viewToken);
                final ViewDefinition viewDefinition = engine.getViewGraph().getViewDefinition(viewToken);
                assertNotNull(viewDefinition);
                final LowerCaseCharSequenceHashSet columns = viewDefinition.getDependencies().get("mv");
                assertNotNull(columns);
                assertTrue(columns.contains("k"));
                if (expectedPolicyColumn != null) {
                    assertTrue(columns.contains(expectedPolicyColumn));
                }
                assertFalse(columns.contains(stalePolicyColumn));
                if (expectedPolicyColumn == null) {
                    assertQuery("SELECT * FROM v1 ORDER BY k").expectSize().noLeakCheck().returns(expected);
                } else {
                    assertQuery("SELECT * FROM v1 ORDER BY k").noLeakCheck().returns(expected);
                }
            } finally {
                viewModelCompiled.countDown();
                resumeCacheHydration.countDown();
                SqlCompilerImpl.setViewFactoryGenerationBarrier(null);
                TableWriter.setMetadataVersionPublishedBarrier(null);
                applyThread.join(30_000);
            }
        });
    }

    private static void awaitOrThrow(CountDownLatch latch, String what) {
        try {
            if (!latch.await(30, TimeUnit.SECONDS)) {
                throw new AssertionError("timed out waiting to " + what);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }
}
