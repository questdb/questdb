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

package io.questdb.test.cairo.wal;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.std.FilesFacade;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for the opt-in, NON-ATOMIC, disk-bounded arbitrary-DELETE apply path (H1,
 * {@code cairo.wal.delete.disk.bounded=true}, {@code OperationExecutor.replaceWithSurvivorsDiskBounded}).
 * <p>
 * The linchpin is {@link #testPerWindowCommitReappliesIdempotentlyAfterMidLoopCrash} (the SPIKE): unlike the
 * default atomic path (one begin/apply.../finish bracket = one commit), this path commits EVERY window
 * separately at the still-current durable seqTxn {@code S-1} and only advances the durable seqTxn to {@code S}
 * with a final {@code commitSeqTxn}. That makes a mid-apply crash leave a PARTIALLY-deleted table at {@code S-1}.
 * The spike proves the whole delete then re-applies idempotently to the exact NOT-predicate oracle.
 */
public class DeleteDiskBoundedApplyTest extends AbstractCairoTest {

    // The arbitrary DELETE predicate exercised throughout: an all-column residual (no pure time range), so it
    // takes the survivor-replace route the disk-bounded path rewrites. 20 of 144 rows match (x = 7,14,..,140).
    private static final String PRED = "x % 7 = 0";

    @Test
    public void testDependentMatViewForcesAtomicRouteOnMidApplyFailure() throws Exception {
        setProperty(PropertyKey.CAIRO_WAL_DELETE_DISK_BOUNDED, "true");
        setProperty(PropertyKey.CAIRO_WAL_DELETE_ROWS_PER_STEP, "1");

        final boolean[] armed = {false};
        final boolean[] faulted = {false};
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                if (armed[0] && !faulted[0] && Utf8s.containsAscii(name, "1970-01-04")) {
                    faulted[0] = true;
                    return -1;
                }
                return super.openRW(name, opts);
            }
        };

        assertMemoryLeak(ff, () -> {
            createParquetFixture();
            execute("CREATE MATERIALIZED VIEW t_1h AS (SELECT ts, sum(x) AS x FROM t SAMPLE BY 1h) PARTITION BY DAY");
            drainWalAndMatViewQueues();
            final TableToken tableToken = engine.verifyTableName("t");
            final long writerTxnBefore = writerTxn(tableToken);
            execute("CREATE TABLE mv_ref AS (SELECT * FROM t_1h)");

            execute("DELETE FROM t WHERE " + PRED);
            armed[0] = true;
            drainWalQueue();
            armed[0] = false;

            Assert.assertTrue(faulted[0]);
            Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(tableToken));
            Assert.assertEquals(writerTxnBefore, writerTxn(tableToken));
            assertSqlCursors("SELECT * FROM t_ref", "SELECT * FROM t");
            assertSqlCursors("SELECT * FROM mv_ref", "SELECT * FROM t_1h");
            assertQuery("SELECT view_status FROM materialized_views WHERE view_name = 't_1h'")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("view_status\nvalid\n");

            execute("ALTER TABLE t RESUME WAL");
            drainWalAndMatViewQueues();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tableToken));
            assertSqlCursors("SELECT * FROM t_ref WHERE NOT (" + PRED + ")", "SELECT * FROM t");
            assertQuery("SELECT view_status, invalidation_reason FROM materialized_views WHERE view_name = 't_1h'")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("view_status\tinvalidation_reason\ninvalid\tdelete operation\n");
        });
    }

    @Test
    public void testRollbackClearsPendingParquetConversionCleanupBeforeUnrelatedBatch() throws Exception {
        final boolean[] armed = {false};
        final boolean[] faulted = {false};
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long openAppend(LPSZ name) {
                if (armed[0] && !faulted[0] && Utf8s.containsAscii(name, "1970-01-02")) {
                    faulted[0] = true;
                    throw CairoException.partitionManipulationRecoverable().put("injected conversion fault");
                }
                return super.openAppend(name);
            }
        };

        assertMemoryLeak(ff, () -> {
            createParquetFixture();
            final TableToken tableToken = engine.verifyTableName("t");
            try (TableWriter writer = engine.getWriter(tableToken, "test rollback cleanup")) {
                writer.convertPartitionParquetToNative(0, false);
                armed[0] = true;
                try {
                    writer.convertPartitionParquetToNative(86_400_000_000L, false);
                    Assert.fail("expected injected conversion fault");
                } catch (CairoException e) {
                    Assert.assertTrue(e.isWALTolerable());
                }
                armed[0] = false;
                Assert.assertTrue(faulted[0]);
                writer.rollback();

                writer.convertPartitionParquetToNative(172_800_000_000L, false);
                writer.commitPendingParquetToNativeConversions();
            }

            engine.releaseAllReaders();
            engine.releaseAllWriters();
            engine.releaseInactive();
            assertSqlCursors("SELECT * FROM t_ref", "SELECT * FROM t");
            assertQuery("SELECT name, isParquet FROM table_partitions('t')")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            name\tisParquet
                            1970-01-01\ttrue
                            1970-01-02\ttrue
                            1970-01-03\tfalse
                            1970-01-04\ttrue
                            1970-01-05\ttrue
                            1970-01-06\tfalse
                            """);
            Assert.assertEquals(0, countDuplicatePartitionVersionDirs(tableToken));
        });
    }

    @Test
    public void testVolatilePredicateUsesAtomicRouteAndReplaysAfterFault() throws Exception {
        setProperty(PropertyKey.CAIRO_WAL_DELETE_DISK_BOUNDED, "true");
        setProperty(PropertyKey.CAIRO_WAL_DELETE_ROWS_PER_STEP, "1");

        final boolean[] armed = {false};
        final boolean[] faulted = {false};
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                if (armed[0] && !faulted[0] && Utf8s.containsAscii(name, "1970-01-04")) {
                    faulted[0] = true;
                    return -1;
                }
                return super.openRW(name, opts);
            }
        };

        assertMemoryLeak(ff, () -> {
            createParquetFixture();
            final TableToken tableToken = engine.verifyTableName("t");
            final long writerTxnBefore = writerTxn(tableToken);
            final long seed0 = 0x1234_5678_9abc_def0L;
            final long seed1 = 0x0fed_cba9_8765_4321L;
            final Rnd expectedRnd = new Rnd(seed0, seed1);
            final StringBuilder expected = new StringBuilder("x\n");
            for (int x = 1; x <= 144; x++) {
                if (!expectedRnd.nextBoolean()) {
                    expected.append(x).append('\n');
                }
            }

            sqlExecutionContext.getRandom().reset(seed0, seed1);
            execute("DELETE FROM t WHERE rnd_boolean()");
            armed[0] = true;
            drainWalQueue();
            armed[0] = false;

            Assert.assertTrue(faulted[0]);
            Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(tableToken));
            Assert.assertEquals(writerTxnBefore, writerTxn(tableToken));
            assertSqlCursors("SELECT * FROM t_ref", "SELECT * FROM t");

            execute("ALTER TABLE t RESUME WAL");
            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tableToken));
            Assert.assertEquals(writerTxnBefore + 1, writerTxn(tableToken));
            assertQuery("SELECT x FROM t").expectSize().returns(expected.toString());
        });
    }

    @Test
    public void testWrappedVolatilePredicateUsesAtomicRouteAndReplaysAfterFault() throws Exception {
        setProperty(PropertyKey.CAIRO_WAL_DELETE_DISK_BOUNDED, "true");
        setProperty(PropertyKey.CAIRO_WAL_DELETE_ROWS_PER_STEP, "1");

        final boolean[] armed = {false};
        final boolean[] faulted = {false};
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                if (armed[0] && !faulted[0] && Utf8s.containsAscii(name, "1970-01-04")) {
                    faulted[0] = true;
                    return -1;
                }
                return super.openRW(name, opts);
            }
        };

        assertMemoryLeak(ff, () -> {
            createParquetFixture();
            final TableToken tableToken = engine.verifyTableName("t");
            final long writerTxnBefore = writerTxn(tableToken);

            execute("DELETE FROM t WHERE geo_distance_meters(0, 0, rnd_double(), 0) > 50000");
            armed[0] = true;
            drainWalQueue();
            armed[0] = false;

            Assert.assertTrue(faulted[0]);
            Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(tableToken));
            Assert.assertEquals(writerTxnBefore, writerTxn(tableToken));
            assertSqlCursors("SELECT * FROM t_ref", "SELECT * FROM t");

            execute("ALTER TABLE t RESUME WAL");
            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tableToken));
            Assert.assertEquals(writerTxnBefore + 1, writerTxn(tableToken));
            Assert.assertEquals(seqTxn(tableToken), writerTxn(tableToken));
            final long survivorCount = count("SELECT count(*) FROM t");
            Assert.assertTrue("wrapped random DELETE must remove a non-empty subset", survivorCount > 0 && survivorCount < 144);
        });
    }

    @Test
    public void testBoundPredicateUsesPerWindowCommitsAndRetries() throws Exception {
        setProperty(PropertyKey.CAIRO_WAL_DELETE_DISK_BOUNDED, "true");
        setProperty(PropertyKey.CAIRO_WAL_DELETE_ROWS_PER_STEP, "1");

        final boolean[] armed = {false};
        final boolean[] faulted = {false};
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                if (armed[0] && !faulted[0] && Utf8s.containsAscii(name, "1970-01-04")) {
                    faulted[0] = true;
                    return -1;
                }
                return super.openRW(name, opts);
            }
        };

        assertMemoryLeak(ff, () -> {
            createParquetFixture();
            final TableToken tableToken = engine.verifyTableName("t");
            final long writerTxnBefore = writerTxn(tableToken);
            final long seqTxnBefore = seqTxn(tableToken);

            sqlExecutionContext.getBindVariableService().setLong(0, 7);
            sqlExecutionContext.getBindVariableService().setLong("upper", 140);
            execute("DELETE FROM t WHERE x % $1 = 0 AND x <= :upper");
            Assert.assertEquals(seqTxnBefore + 1, seqTxn(tableToken));
            // Mutating the submit context after enqueue verifies that apply uses the values captured in WAL.
            sqlExecutionContext.getBindVariableService().setLong(0, 5);
            sqlExecutionContext.getBindVariableService().setLong("upper", 20);

            armed[0] = true;
            drainWalQueue();
            armed[0] = false;

            Assert.assertTrue("the bound DELETE must reach the disk-bounded loop", faulted[0]);
            Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(tableToken));
            Assert.assertEquals(writerTxnBefore, writerTxn(tableToken));
            final long remainingMatched = count("SELECT count(*) FROM t WHERE " + PRED);
            Assert.assertTrue(
                    "per-window commits must leave a partial bound DELETE after the fault",
                    remainingMatched > 0 && remainingMatched < 20
            );

            execute("ALTER TABLE t RESUME WAL");
            drainWalQueue();

            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tableToken));
            Assert.assertEquals(writerTxnBefore + 1, writerTxn(tableToken));
            Assert.assertEquals(seqTxn(tableToken), writerTxn(tableToken));
            assertSqlCursors("SELECT * FROM t_ref WHERE NOT (" + PRED + ")", "SELECT * FROM t");
        });
    }

    /**
     * SPIKE (gate). A mid-loop crash of the per-window-commit scheme must re-apply the WHOLE delete idempotently.
     * <p>
     * Fixture: 144 hourly rows over 6 daily partitions; days 1-5 Parquet, day 6 (active) native. With
     * {@code disk.bounded=true} and {@code rows.per.step} tiny, the arbitrary DELETE tiles into MANY per-window
     * commits at seqTxn S-1. A {@link FilesFacade} fails the first native write to the day-4 partition
     * ({@code 1970-01-04}) exactly once, AFTER days 1-3's windows have each committed at S-1 - a genuine mid-loop
     * crash (the throw rolls back the in-flight window; executeDelete's catch sets seqTxn back to S-1; the table
     * suspends). We then assert the delete is only PARTIALLY applied (non-atomicity is observable) and the durable
     * seqTxn is still S-1, disarm the fault, resume + re-drain (the crash-restart model), and assert the whole
     * delete re-applied to the NOT-predicate oracle, the table is healthy, and the durable seqTxn advanced by
     * exactly 1.
     */
    @Test
    public void testPerWindowCommitReappliesIdempotentlyAfterMidLoopCrash() throws Exception {
        setProperty(PropertyKey.CAIRO_WAL_DELETE_DISK_BOUNDED, "true");
        setProperty(PropertyKey.CAIRO_WAL_DELETE_ROWS_PER_STEP, "1"); // tiny step -> ~1 window per hourly row

        // Fails the FIRST native openRW that targets the day-4 partition, but only while armed (i.e. during the
        // DELETE apply, not during table setup / Parquet conversion). Days 1-3 commit first, so this is a genuine
        // mid-loop crash.
        final boolean[] armed = {false};
        final boolean[] faulted = {false};
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                if (armed[0] && !faulted[0] && Utf8s.containsAscii(name, "1970-01-04")) {
                    faulted[0] = true;
                    return -1; // simulate a disk failure writing the day-4 native files (convert or replace)
                }
                return super.openRW(name, opts);
            }
        };

        assertMemoryLeak(ff, () -> {
            createParquetFixture();
            final TableToken tt = engine.verifyTableName("t");
            final long writerTxnBefore = writerTxn(tt);
            final long seqTxnBefore = seqTxn(tt);

            execute("delete from t where " + PRED);
            // The delete is now one enqueued sequencer txn S = (S-1)+1, not yet applied.
            Assert.assertEquals("sequencer must have exactly one new (delete) txn", seqTxnBefore + 1, seqTxn(tt));

            // Crash mid-apply: arm the fault and drain. Day-4's convert/replace fails after days 1-3 committed.
            armed[0] = true;
            drainWalQueue();
            armed[0] = false;

            Assert.assertTrue("the mid-loop fault must have actually fired", faulted[0]);
            Assert.assertTrue("a mid-loop crash must suspend the table (delete not yet complete)",
                    engine.getTableSequencerAPI().isSuspended(tt));
            // Durable seqTxn is STILL S-1: the per-window commits persisted S-1, the final commitSeqTxn(S) never
            // ran. So the delete is not durably applied even though earlier windows physically committed.
            Assert.assertEquals("durable seqTxn must remain S-1 after a mid-loop crash",
                    writerTxnBefore, writerTxn(tt));

            // NON-ATOMICITY / partial commit is observable: some matched rows are already deleted (days 1-3
            // windows committed at S-1), but not all (day-4+ never applied). Robust to exact window/day alignment.
            final long remainingMatched = count("select count(*) from t where " + PRED);
            Assert.assertTrue(
                    "a mid-loop crash must leave a PARTIAL delete: 0 < remaining matched (" + remainingMatched + ") < 20",
                    remainingMatched > 0 && remainingMatched < 20
            );
            final long total = count("select count(*) from t");
            Assert.assertTrue(
                    "partial delete: 124 (all deleted) < total (" + total + ") < 144 (none deleted)",
                    total > 124 && total < 144
            );

            // Crash-restart model: resume the suspended table and re-drain. executeDelete re-runs the WHOLE delete
            // for txn S over the partially-committed (still-at-S-1) table.
            execute("alter table t resume wal");
            drainWalQueue();

            Assert.assertFalse("re-apply must leave the table healthy (not suspended)",
                    engine.getTableSequencerAPI().isSuspended(tt));
            // Durable seqTxn advanced by EXACTLY 1 for the delete (S-1 -> S).
            Assert.assertEquals("durable seqTxn must have advanced by exactly 1 for the delete",
                    writerTxnBefore + 1, writerTxn(tt));
            Assert.assertEquals("durable seqTxn must match the sequencer after full apply",
                    seqTxn(tt), writerTxn(tt));
            // Idempotent re-apply: final table is exactly the NOT-predicate survivor set, in table order.
            assertSqlCursors("select * from t_ref where not (" + PRED + ")", "select * from t");
        });
    }

    /**
     * T6-harden guard. A WAL-tolerable {@link CairoException} thrown MID-LOOP on the disk-bounded path must
     * SUSPEND the table at the durable seqTxn {@code S-1} (so {@code ApplyWal2TableJob} re-runs the WHOLE
     * delete idempotently) and must NOT be finalized. A WAL-tolerable error means "skip this txn, mark it
     * applied" ({@code commitSeqTxn(S)}); that is correct only on the atomic routes (nothing durably committed
     * yet), but on the disk-bounded route earlier windows already committed at {@code S-1}, so finalizing at
     * {@code S} here would silently mark a PARTIALLY-applied delete complete = data loss.
     * <p>
     * The guard is TWO parts in {@code executeDelete}'s catch, BOTH required, because a WAL-tolerable error is
     * skipped-and-finalized at TWO layers: (1) that catch's own {@code !isDiskBounded} branch stops IT from
     * finalizing; (2) but the CALLER {@code ApplyWal2TableJob.processWalSql} has its own
     * {@code catch (CairoException)} that ALSO {@code commitSeqTxn(seqTxn)}'s any rethrown WAL-tolerable error -
     * so on the disk-bounded route the catch additionally rethrows it as a CRITICAL error, the only kind that
     * caller rethrows-and-suspends. (A guard that only did (1) and rethrew the WAL-tolerable error as-is would
     * STILL be silently finalized by (2) - verified during development.)
     * <p>
     * Unlike the {@link #testPerWindowCommitReappliesIdempotentlyAfterMidLoopCrash} spike (which makes
     * {@code openRW} RETURN -1 -> a CRITICAL, non-WAL-tolerable error that already suspends safely), this test
     * makes the {@link FilesFacade} THROW a genuine WAL-tolerable {@code CairoException}
     * ({@link CairoException#partitionManipulationRecoverable()}, whose {@code isWALTolerable()==true}) at the
     * same day-4 point, so it reaches the catch as WAL-tolerable and exercises the guard specifically.
     */
    @Test
    public void testDiskBoundedWalTolerableMidLoopSuspendsNotFinalizes() throws Exception {
        setProperty(PropertyKey.CAIRO_WAL_DELETE_DISK_BOUNDED, "true");
        setProperty(PropertyKey.CAIRO_WAL_DELETE_ROWS_PER_STEP, "1"); // tiny step -> ~1 window per hourly row

        // Throws a WAL-TOLERABLE CairoException (partition-manipulation-recoverable) on the FIRST native openAppend
        // targeting the day-4 partition, but only while armed (during DELETE apply). Days 1-3 commit first, so this
        // is a genuine mid-loop crash that reaches executeDelete's catch with isWALTolerable()==true.
        //
        // Why openAppend (not openRW like the spike): the day-4 window first CONVERTS its Parquet partition to
        // native (convertParquetPartitionsForDeleteWindow -> produceNativeFromParquet), which writes the native
        // column files via ff.openAppend; that call site propagates a thrown CairoException UNCHANGED
        // (produceNativeFromParquet's catch and convertPartitionParquetToNative's catch both rethrow it as-is), so
        // a WAL-tolerable errno survives to executeDelete's catch. The spike's openRW=-1 instead fires later, in the
        // window's O3 replaceRange, whose machinery re-wraps the failure into a CRITICAL (non-WAL-tolerable) errno -
        // which would NOT exercise this guard.
        final boolean[] armed = {false};
        final boolean[] faulted = {false};
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long openAppend(LPSZ name) {
                if (armed[0] && !faulted[0] && Utf8s.containsAscii(name, "1970-01-04")) {
                    faulted[0] = true;
                    throw CairoException.partitionManipulationRecoverable()
                            .put("injected WAL-tolerable fault writing day-4 native files");
                }
                return super.openAppend(name);
            }
        };

        assertMemoryLeak(ff, () -> {
            createParquetFixture();
            final TableToken tt = engine.verifyTableName("t");
            final long writerTxnBefore = writerTxn(tt);
            final long seqTxnBefore = seqTxn(tt);

            execute("delete from t where " + PRED);
            Assert.assertEquals("sequencer must have exactly one new (delete) txn", seqTxnBefore + 1, seqTxn(tt));

            // Crash mid-apply: arm the fault and drain. Day-4's convert/replace throws a WAL-tolerable error
            // after days 1-3 committed at S-1.
            armed[0] = true;
            drainWalQueue();
            armed[0] = false;

            Assert.assertTrue("the mid-loop WAL-tolerable fault must have actually fired", faulted[0]);

            // The guard forces NOT-applied + suspend: a WAL-tolerable error on the disk-bounded route must NOT be
            // finalized (finalizing at S would mark the PARTIALLY-applied delete complete = silent data loss). So the
            // table SUSPENDS and the durable seqTxn stays S-1 (final commitSeqTxn(S) never ran) - the delete is not
            // durably applied and ApplyWal2TableJob can re-run it.
            Assert.assertTrue("a WAL-tolerable mid-loop error on the disk-bounded path must SUSPEND (not finalize)",
                    engine.getTableSequencerAPI().isSuspended(tt));
            Assert.assertEquals("durable seqTxn must remain S-1 after a suspended mid-loop WAL-tolerable error",
                    writerTxnBefore, writerTxn(tt));

            // NON-ATOMICITY / partial commit is observable: days 1-3 windows committed at S-1, day-4+ never applied.
            final long remainingMatched = count("select count(*) from t where " + PRED);
            Assert.assertTrue(
                    "a mid-loop crash must leave a PARTIAL delete: 0 < remaining matched (" + remainingMatched + ") < 20",
                    remainingMatched > 0 && remainingMatched < 20
            );

            // Crash-restart model: resume the suspended table and re-drain. executeDelete re-runs the WHOLE delete
            // for txn S over the partially-committed (still-at-S-1) table - the guard is what made this retry
            // reachable instead of a silent finalize.
            execute("alter table t resume wal");
            drainWalQueue();

            Assert.assertFalse("re-apply must leave the table healthy (not suspended)",
                    engine.getTableSequencerAPI().isSuspended(tt));
            // Durable seqTxn advanced by EXACTLY 1 for the delete (S-1 -> S).
            Assert.assertEquals("durable seqTxn must have advanced by exactly 1 for the delete",
                    writerTxnBefore + 1, writerTxn(tt));
            Assert.assertEquals("durable seqTxn must match the sequencer after full apply",
                    seqTxn(tt), writerTxn(tt));
            // Idempotent re-apply: final table is exactly the NOT-predicate survivor set, in table order.
            assertSqlCursors("select * from t_ref where not (" + PRED + ")", "select * from t");
        });
    }

    /**
     * D6-Important (whole-PR level-3): the DEFAULT (atomic) route's Parquet convert->replace TWO-COMMIT crash
     * window. {@code executeDelete}'s atomic route does {@code convertParquetPartitionsForDelete} - its own
     * physical commit #1 at the STILL-CURRENT durable seqTxn {@code S-1}, un-tiering every Parquet partition to
     * native (verified: {@code produceNativeFromParquet} writes native columns via {@code ff.openAppend} and
     * reads the Parquet via {@code mapRO}) - and THEN {@code replaceWithSurvivors} (commit #2, the ONLY advance to
     * {@code S}, whose O3 partition writes go via {@code ff.openRW}). A crash AFTER commit #1 but BEFORE commit #2
     * must leave the table SUSPENDED at durable {@code S-1} with the convert landed and the data fully intact (the
     * convert is format-only, the delete never applied); the re-run's re-issued convert on the now-native
     * partitions is an idempotent no-op and the replace then completes. The disk-bounded route's analogous window
     * is covered by {@link #testPerWindowCommitReappliesIdempotentlyAfterMidLoopCrash}; this is the atomic
     * route's coverage, which was missing.
     * <p>
     * Injection point (distinct from the convert's {@code openAppend}): {@code openRW=-1} on day-4 fires only in
     * the REPLACE's O3 write to day-4, after commit #1 has landed - never during the convert. Self-verifying: the
     * test asserts commit #1 DID land (no Parquet partition remains at {@code S-1}) and commit #2 did NOT (durable
     * still {@code S-1}, data == full pre-delete snapshot).
     */
    @Test
    public void testAtomicRouteConvertCommitThenReplaceCrashSuspendsAtSMinus1() throws Exception {
        setProperty(PropertyKey.CAIRO_WAL_DELETE_DISK_BOUNDED, "false"); // DEFAULT atomic route (convert pre-pass + one replace bracket)
        setProperty(PropertyKey.CAIRO_WAL_DELETE_ROWS_PER_STEP, "1"); // tiny step -> the day-4 replace is its own window

        // Fails the FIRST openRW targeting day-4 while armed. The convert (commit #1) writes native columns via
        // openAppend and reads Parquet via mapRO, so this openRW=-1 fires only later, in the replace's O3 write to
        // day-4 - i.e. strictly AFTER commit #1 has landed.
        final boolean[] armed = {false};
        final boolean[] faulted = {false};
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                if (armed[0] && !faulted[0] && Utf8s.containsAscii(name, "1970-01-04")) {
                    faulted[0] = true;
                    return -1; // fail the replace's native O3 write to day-4, after the convert commit
                }
                return super.openRW(name, opts);
            }
        };

        assertMemoryLeak(ff, () -> {
            createParquetFixture(); // days 1-5 Parquet, day 6 native; t_ref = full pre-delete native snapshot
            final TableToken tt = engine.verifyTableName("t");
            final long writerTxnBefore = writerTxn(tt);
            final long seqTxnBefore = seqTxn(tt);

            execute("delete from t where " + PRED);
            Assert.assertEquals("sequencer must have exactly one new (delete) txn", seqTxnBefore + 1, seqTxn(tt));

            // Crash between commit #1 (convert) and commit #2 (replace): arm the fault and drain.
            armed[0] = true;
            drainWalQueue();
            armed[0] = false;

            Assert.assertTrue("the replace-write fault must have actually fired", faulted[0]);
            Assert.assertTrue("a crash before the replace commit must suspend the table",
                    engine.getTableSequencerAPI().isSuspended(tt));
            // Durable seqTxn is STILL S-1: commit #1 (convert) persisted at S-1, commit #2 (replace) never ran.
            Assert.assertEquals("durable seqTxn must remain S-1 (the replace commit never landed)",
                    writerTxnBefore, writerTxn(tt));

            // The TWO-COMMIT structure is observable: commit #1 (Parquet->native convert) DID land - no Parquet
            // partition remains at S-1, proving the convert committed before the replace faulted.
            Assert.assertEquals(
                    "commit #1 (Parquet->native convert) must have landed at S-1: no Parquet partition should remain",
                    0,
                    count("select count(*) from table_partitions('t') where isParquet")
            );
            // Data fully intact: the convert is format-only and the delete never applied, so the table still
            // equals the full pre-delete snapshot (nothing deleted yet).
            assertSqlCursors("select * from t_ref", "select * from t");

            // Crash-restart model: resume + re-drain. The re-run's convert on now-native partitions is an
            // idempotent no-op, and the replace (openRW no longer armed) completes commit #2 (S-1 -> S).
            execute("alter table t resume wal");
            drainWalQueue();

            Assert.assertFalse("re-apply must leave the table healthy (not suspended)",
                    engine.getTableSequencerAPI().isSuspended(tt));
            Assert.assertEquals("durable seqTxn must have advanced by exactly 1 for the delete (S-1 -> S)",
                    writerTxnBefore + 1, writerTxn(tt));
            Assert.assertEquals("durable seqTxn must match the sequencer after full apply",
                    seqTxn(tt), writerTxn(tt));
            // Final state == the exact NOT-predicate oracle: the re-run's convert-no-op + replace reached it.
            assertSqlCursors("select * from t_ref where not (" + PRED + ")", "select * from t");

            // F-E: the crash-then-resume path must not leave orphaned uncommitted partition-version dirs. The
            // mid-replace fault triggers abortReplaceRange's rollback; the resumed re-apply then completes the
            // delete. A regression where the rollback (or the re-apply) leaks a superseded/uncommitted version
            // dir would still pass the data oracle above (the leaked dir is detached + invisible to queries),
            // so apply the same on-disk duplicate-version-dir oracle the successful multi-window path uses.
            Assert.assertEquals(
                    "no partition may have a duplicate (orphaned) physical version dir after crash+resume",
                    0,
                    countDuplicatePartitionVersionDirs(tt)
            );
        });
    }

    // Happy path: arbitrary DELETE over the all-Parquet (except mandatory-native active day 6) table with a tiny
    // rows-per-step, so it tiles into MANY per-window commits. Final state == NOT-predicate oracle, table healthy.
    @Test
    public void testDiskBoundedManyWindowsMatchesOracle() throws Exception {
        setProperty(PropertyKey.CAIRO_WAL_DELETE_DISK_BOUNDED, "true");
        setProperty(PropertyKey.CAIRO_WAL_DELETE_ROWS_PER_STEP, "1"); // ~1 window per hourly row -> many windows
        assertMemoryLeak(() -> {
            createParquetFixture();
            final TableToken tt = engine.verifyTableName("t");
            execute("delete from t where " + PRED);
            drainWalQueue();
            Assert.assertFalse("a valid arbitrary DELETE must not suspend the table",
                    engine.getTableSequencerAPI().isSuspended(tt));
            assertSqlCursors("select * from t_ref where not (" + PRED + ")", "select * from t");
        });
    }

    // Single-window disk-bounded path: a rows-per-step larger than the whole table collapses to ONE window (one
    // replaceRange commit + the final commitSeqTxn). Must match the same oracle and not suspend.
    @Test
    public void testDiskBoundedSingleWindowMatchesOracle() throws Exception {
        setProperty(PropertyKey.CAIRO_WAL_DELETE_DISK_BOUNDED, "true");
        setProperty(PropertyKey.CAIRO_WAL_DELETE_ROWS_PER_STEP, "100000000"); // >> table size -> one window
        assertMemoryLeak(() -> {
            createParquetFixture();
            final TableToken tt = engine.verifyTableName("t");
            execute("delete from t where " + PRED);
            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tt));
            assertSqlCursors("select * from t_ref where not (" + PRED + ")", "select * from t");
        });
    }

    /**
     * Finding #5 regression: the ACTIVE (last) partition CAN be Parquet on a WAL table, so
     * {@code convertParquetPartitionsForDeleteWindow}'s last-partition branch (which bounds the last partition's
     * extent by {@code getMaxTimestamp()+1}) must handle a Parquet active partition that spans several windows.
     * The active-partition skip in {@code TableWriter.convertPartitionNativeToParquet} is gated on
     * {@code !isWal()}, so {@code ALTER TABLE ... CONVERT PARTITION TO PARQUET} over a range that includes the
     * active day DOES convert it on a WAL table (unlike a non-WAL table, which skips it) - which a stale comment
     * on that branch wrongly claimed could "never be Parquet". The {@code getMaxTimestamp()+1} bound is sound
     * whether or not the last partition is Parquet, so this is a coverage test for the previously-untested
     * active-Parquet path, NOT a data-bug repro.
     * <p>
     * Fixture: 144 hourly rows over 6 daily partitions, ALL converted to Parquet INCLUDING the active day 6;
     * with {@code rows.per.step=1} the active day alone spans many per-window commits. The test first asserts the
     * active/last partition really is Parquet (the precondition this test exists to exercise), then runs the
     * arbitrary DELETE, drains, and checks not-suspended + the exact NOT-predicate oracle.
     */
    @Test
    public void testDiskBoundedActiveParquetPartitionMultiWindow() throws Exception {
        setProperty(PropertyKey.CAIRO_WAL_DELETE_DISK_BOUNDED, "true");
        setProperty(PropertyKey.CAIRO_WAL_DELETE_ROWS_PER_STEP, "1"); // ~1 window per hourly row -> active day spans many
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, x long) timestamp(ts) partition by DAY WAL");
            execute("insert into t select timestamp_sequence('1970-01-01T00:00:00.000000Z', 60*60*1000000L), x from long_sequence(144)");
            drainWalQueue();
            execute("create table t_ref as (select * from t)");
            // Convert EVERY partition, INCLUDING the active day 6 (ts < day 7 covers all 6 days). On a WAL table
            // the active partition IS converted (a non-WAL table would skip it), so day 6 becomes Parquet.
            execute("alter table t convert partition to parquet where ts < '1970-01-07T00:00:00.000000Z'");
            drainWalQueue();

            final TableToken tt = engine.verifyTableName("t");
            // Precondition (self-verifying fixture): the ACTIVE/last partition (day 6) must actually be Parquet -
            // the whole point of this test. If this ever fails, the active-Parquet path is no longer exercised.
            Assert.assertEquals(
                    "the active/last partition (day 6) must actually be Parquet on a WAL table",
                    1,
                    count("select count(*) from table_partitions('t') where name = '1970-01-06' and isParquet")
            );

            execute("delete from t where " + PRED);
            drainWalQueue();

            Assert.assertFalse("a valid arbitrary DELETE over an active-Parquet table must not suspend",
                    engine.getTableSequencerAPI().isSuspended(tt));
            assertSqlCursors("select * from t_ref where not (" + PRED + ")", "select * from t");
        });
    }

    /**
     * F-F coverage: the ATOMIC route (default cairo.wal.delete.disk.bounded=false) over a table whose ACTIVE/last
     * partition is Parquet, with the delete spanning >= 2 windows over it. Active-Parquet-across-windows was
     * covered only on the disk-bounded route (testDiskBoundedActiveParquetPartitionMultiWindow); the atomic route
     * converts all in-range Parquet in ONE pre-pass (commit #1) before the single replace bracket - lower-risk
     * than the per-window disk-bounded convert, but previously untested (DeleteTest converts only an INTERIOR
     * day). This is the atomic-route counterpart of the disk-bounded active-Parquet test.
     */
    @Test
    public void testAtomicRouteActiveParquetPartitionMultiWindow() throws Exception {
        setProperty(PropertyKey.CAIRO_WAL_DELETE_DISK_BOUNDED, "false"); // DEFAULT atomic route (convert pre-pass + one replace bracket)
        setProperty(PropertyKey.CAIRO_WAL_DELETE_ROWS_PER_STEP, "1"); // ~1 window per hourly row -> the active day spans many
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, x long) timestamp(ts) partition by DAY WAL");
            execute("insert into t select timestamp_sequence('1970-01-01T00:00:00.000000Z', 60*60*1000000L), x from long_sequence(144)");
            drainWalQueue();
            execute("create table t_ref as (select * from t)");
            // Convert EVERY partition, INCLUDING the active day 6 (ts < day 7 covers all 6 days). On a WAL table
            // the active partition IS converted (a non-WAL table would skip it), so day 6 becomes Parquet.
            execute("alter table t convert partition to parquet where ts < '1970-01-07T00:00:00.000000Z'");
            drainWalQueue();

            final TableToken tt = engine.verifyTableName("t");
            // Precondition (self-verifying fixture): the ACTIVE/last partition (day 6) must actually be Parquet.
            Assert.assertEquals(
                    "the active/last partition (day 6) must actually be Parquet on a WAL table",
                    1,
                    count("select count(*) from table_partitions('t') where name = '1970-01-06' and isParquet")
            );

            // Arbitrary residual predicate (x % 7 = 0) matches rows in the active Parquet day (x=126,133,140),
            // so with rows.per.step=1 the delete tiles that partition across many windows.
            execute("delete from t where " + PRED);
            drainWalQueue();

            Assert.assertFalse("a valid arbitrary DELETE over an active-Parquet table (atomic route) must not suspend",
                    engine.getTableSequencerAPI().isSuspended(tt));
            assertSqlCursors("select * from t_ref where not (" + PRED + ")", "select * from t");
        });
    }

    // No-match arbitrary DELETE: the survivor-replace rewrites every window with ALL rows (removes nothing). The
    // whole table survives and the table stays healthy (still un-tiers Parquet as a side effect, as documented).
    @Test
    public void testDiskBoundedNoMatchRemovesNothing() throws Exception {
        setProperty(PropertyKey.CAIRO_WAL_DELETE_DISK_BOUNDED, "true");
        setProperty(PropertyKey.CAIRO_WAL_DELETE_ROWS_PER_STEP, "1");
        assertMemoryLeak(() -> {
            createParquetFixture();
            final TableToken tt = engine.verifyTableName("t");
            // Residual (non-time-range) predicate that matches no row -> arbitrary route, zero survivors removed.
            execute("delete from t where x < 0");
            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tt));
            assertSqlCursors("select * from t_ref", "select * from t");
        });
    }

    // The disk.bounded=FALSE counterpart of testDiskBoundedManyWindowsMatchesOracle: the default atomic path over
    // the identical fixture + predicate reaches the SAME end state (the shared NOT-predicate oracle). Together the
    // two tests prove the disk-bounded and atomic routes agree on the final result.
    @Test
    public void testDiskBoundedFalseSameEndStateAsOracle() throws Exception {
        setProperty(PropertyKey.CAIRO_WAL_DELETE_DISK_BOUNDED, "false"); // default atomic windowed path
        setProperty(PropertyKey.CAIRO_WAL_DELETE_ROWS_PER_STEP, "1");
        assertMemoryLeak(() -> {
            createParquetFixture();
            final TableToken tt = engine.verifyTableName("t");
            execute("delete from t where " + PRED);
            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tt));
            assertSqlCursors("select * from t_ref where not (" + PRED + ")", "select * from t");
        });
    }

    // 144 hourly rows over 6 daily partitions; days 1-5 converted to Parquet, day 6 (active) stays native.
    // t_ref is an independent native oracle snapshot of the full pre-delete data.
    private void createParquetFixture() throws Exception {
        execute("create table t (ts timestamp, x long) timestamp(ts) partition by DAY WAL");
        execute("insert into t select timestamp_sequence('1970-01-01T00:00:00.000000Z', 60*60*1000000L), x from long_sequence(144)");
        drainWalQueue();
        execute("create table t_ref as (select * from t)");
        // Convert every partition except the active last one (day 6) to Parquet.
        execute("alter table t convert partition to parquet where ts < '1970-01-06T00:00:00.000000Z'");
        drainWalQueue();
    }

    private long count(String sql) throws Exception {
        try (RecordCursorFactory factory = select(sql, sqlExecutionContext);
             RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            return cursor.hasNext() ? cursor.getRecord().getLong(0) : -1;
        }
    }

    // Counts SUPERSEDED partition-version directories physically present under the table dir: for each calendar-day
    // partition (dir named yyyy-MM-dd, optionally with a .<nameTxn> version suffix), every physical version dir
    // beyond the first for that day is a duplicate = a superseded/orphaned version not yet reclaimed. A correct
    // apply leaves exactly one physical dir per calendar day, so this returns 0. Replicated from
    // DeleteWindowedApplyTest#countDuplicatePartitionVersionDirs per the F-E brief (small helper, kept local).
    private int countDuplicatePartitionVersionDirs(TableToken tableToken) {
        final FilesFacade ff = configuration.getFilesFacade();
        final java.util.HashSet<String> seenDays = new java.util.HashSet<>();
        final int[] duplicates = {0};
        try (Path path = new Path()) {
            path.of(configuration.getDbRoot()).concat(tableToken);
            final int plen = path.size();
            ff.iterateDir(path.$(), (pUtf8NameZ, type) -> {
                if (ff.isDirOrSoftLinkDirNoDots(path, plen, pUtf8NameZ, type)) {
                    final byte first = Unsafe.getByte(pUtf8NameZ);
                    if (first >= '0' && first <= '9') {
                        final String name = path.toString().substring(plen + 1);
                        final int dot = name.indexOf('.');
                        final String day = dot < 0 ? name : name.substring(0, dot);
                        if (!seenDays.add(day)) {
                            duplicates[0]++;
                        }
                    }
                    path.trimTo(plen);
                }
            });
        }
        return duplicates[0];
    }

    private long seqTxn(TableToken tt) {
        final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(tt);
        return tracker.getSeqTxn();
    }

    private long writerTxn(TableToken tt) {
        final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(tt);
        return tracker.getWriterTxn();
    }
}
