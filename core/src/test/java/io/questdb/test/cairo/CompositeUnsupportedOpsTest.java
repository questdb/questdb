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

package io.questdb.test.cairo;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import org.junit.Assert;
import org.junit.Test;

/**
 * Plan 4a DEFINITIVE cell-blind-path convergence sweep. This is the convergence proof: it enumerates
 * every partition-touching DDL/maintenance operation the sweep covered and, for each one that a
 * routed (real, non-dormant) composite table cannot yet execute safely, asserts it now throws a
 * clear, consistently-worded "composite partitioning does not yet support &lt;OP&gt;" error instead
 * of silently corrupting data, hanging, or crashing with a confusing internal message. It also
 * asserts every op that genuinely IS cell-correct today (TRUNCATE, VACUUM, plain INSERT/SELECT)
 * keeps working, and that a PLAIN table is completely unaffected by any of the new gates.
 * <p>
 * Every gated op below was independently confirmed SILENT or CRASH-prone on a routed 2-cell
 * composite table (one day with two distinct dimension values, {@code exch='A'}/{@code 'B'}, routed
 * in a single commit -- the safe multi-cell shape {@code CompositeRoutingTest}'s own acceptance tests
 * already prove routes correctly) before its gate was added. The op-by-op evidence includes
 * a live-reproduced INFINITE LOOP for DROP PARTITION (not just a clean crash).
 */
public class CompositeUnsupportedOpsTest extends AbstractCairoTest {

    // ------------------------------------------------------------------------------------------
    // GATED: every op below must throw "composite partitioning does not yet support <OP>" on a
    // real routed composite table.
    // ------------------------------------------------------------------------------------------

    /**
     * The cell-qualified DROP gate is gone: sub-project 1C implemented what it refused. The refusal
     * existed because 1B measured {@code DROP PARTITION LIST '<day>/E0'} taking a three-cell day to
     * EMPTY; per-cell removal now does exactly what the statement names. Coverage lives in
     * {@code CompositeDropPartitionWholeDayTest}.
     */

    /**
     * SP1 (2026-08-18): DETACH PARTITION is no longer gated for composite tables. It detaches the day
     * as a container holding its cells; the round-trip back via ATTACH is still unsupported and gated
     * separately. Full behaviour lives in CompositeDetachAttachTest.
     */
    @Test
    public void testDetachPartitionIsNoLongerGated() throws Exception {
        assertMemoryLeak(() -> {
            createRoutedTwoCellTable("c");
            execute("insert into c values ('2020-01-02T00:00:00.000000Z','A',2.0)");
            drainWalQueue();
            execute("alter table c detach partition list '2020-01-01'");
            drainWalQueue();
            assertWalTableNotSuspended("c");
        });
    }

    @Test
    public void testAttachPartitionGated() throws Exception {
        assertMemoryLeak(() -> {
            createRoutedTwoCellTable("c");
            // No real ".detached" source directory is needed: the gate fires unconditionally at the
            // very top of TableWriter#attachPartition, before any lookup of a real detached source.
            assertCompositeGateFires(
                    "alter table c attach partition list '2020-01-01'",
                    "c",
                    "composite partitioning does not yet support ATTACH PARTITION");
        });
    }

    @Test
    public void testConvertPartitionToParquetGated() throws Exception {
        assertMemoryLeak(() -> {
            createRoutedTwoCellTable("c");
            assertCompositeGateFires(
                    "alter table c convert partition to parquet list '2020-01-01'",
                    "c",
                    "composite partitioning does not yet support CONVERT PARTITION TO PARQUET");
        });
    }

    @Test
    public void testConvertPartitionToNativeGated() throws Exception {
        assertMemoryLeak(() -> {
            createRoutedTwoCellTable("c");
            // The gate fires unconditionally, before checking whether the partition is actually
            // parquet-format yet -- CONVERT TO PARQUET is itself gated, so a real composite table can
            // never legitimately reach parquet format via ordinary SQL in the first place.
            assertCompositeGateFires(
                    "alter table c convert partition to native list '2020-01-01'",
                    "c",
                    "composite partitioning does not yet support CONVERT PARTITION TO NATIVE");
        });
    }

    /**
     * SP1E (2026-08-18): SQUASH PARTITIONS is no longer gated for composite tables. Mid-table
     * split-fragment squash is implemented and cell-scoped (see CompositeSquashTest); an active-tail
     * day group is SKIPPED with a log line rather than refused, so there is no gate left to fire.
     */
    @Test
    public void testSquashPartitionsIsNoLongerGated() throws Exception {
        assertMemoryLeak(() -> {
            createRoutedTwoCellTable("c");
            execute("alter table c squash partitions");
            drainWalQueue();
            assertWalTableNotSuspended("c");
        });
    }

    /**
     * Plan 4b Task 1b. FORCE DROP PARTITION is the ungated sibling of (plain) DROP PARTITION --
     * {@code AlterOperation#isForceWalBypass} routes it around the WAL entirely (applied synchronously,
     * bypassing the sequencer), so it needed its own dedicated gate; {@link #testDropPartitionGated()}
     * alone never exercised it.
     */
    /**
     * FORCE DROP PARTITION became SUPPORTED on composite tables in sub-project 1D, so the gate this
     * test asserted is gone. Coverage moved to {@code CompositeTtlAndForceDropTest}. A cell-qualified
     * name needs no gate here: this statement's LIST parser rejects it with a date-format error, which
     * is why FORCE DROP required no equivalent of 1B's refuseCellQualifiedPartitionName.
     */

    /**
     * Plan 4b Task 1b. TTL eviction shares {@code dropPartitionByExactTimestamp}'s cell-blind selection
     * chain with (plain) DROP PARTITION -- the same infinite-loop/sibling-deletion risk {@link
     * #testDropPartitionGated()}'s own gate was added for, but reached via {@code ALTER TABLE ... SET TTL}
     * instead, which was left ungated until now.
     */
    /**
     * TTL eviction became SUPPORTED on composite tables in sub-project 1D, so the gate this test
     * asserted is gone. Coverage moved to {@code CompositeTtlAndForceDropTest}, which proves the
     * stronger property: a composite table evicts the same days as its plain twin, through the path
     * that actually evicts (a COMMIT, not the SET TTL statement).
     */

    @Test
    public void testAlterColumnTypeGated() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table c (ts timestamp, exch symbol, note varchar, px double) timestamp(ts) partition by day, exch wal");
            // Two SEPARATE single-cellKey commits (not one interleaved commit) -- a table with a
            // var-size column hits a DIFFERENT, pre-existing, out-of-scope guard ("an interleaved
            // multi-cell commit is not yet supported for a table with a var-size column",
            // TableWriter.java ~10966) if a single dispatch batch spans 2+ distinct cellKeys. Splitting
            // into two commits (the same shape CompositeRoutingTest's own
            // testMultiCommitAddsSecondCellToSingleCellDayMatchesPlainTwin proves safe) reaches a real,
            // routed, 2-cell composite table without tripping that unrelated guard.
            execute("insert into c values ('2020-01-01T00:00:00.000000Z','A','1',1.0)");
            drainWalQueue();
            execute("insert into c values ('2020-01-01T12:00:00.000000Z','B','2',1.5)");
            drainWalQueue();
            assertWalTableNotSuspended("c");
            assertCompositeGateFires(
                    "alter table c alter column note type symbol",
                    "c",
                    "composite partitioning does not yet support ALTER COLUMN TYPE");
        });
    }

    @Test
    public void testUpdateGated() throws Exception {
        assertMemoryLeak(() -> {
            createRoutedTwoCellTable("c");
            assertCompositeGateFires(
                    "update c set px = px + 1",
                    "c",
                    // PERMANENT as of 2026-08-18 -- "does not support", not "does not yet".
                    "composite partitioning does not support UPDATE");
        });
    }

    /**
     * SP2 (2026-08-18): ADD INDEX is no longer gated. Note the column here is the DIMENSION column
     * itself, where every row in a given cell shares one value -- a degenerate but legitimate index,
     * and worth asserting rather than assuming, since the index build now walks cells.
     */
    @Test
    public void testAddIndexOnDimensionColumnWorks() throws Exception {
        assertMemoryLeak(() -> {
            createRoutedTwoCellTable("c");
            execute("alter table c alter column exch add index");
            drainWalQueue();
            assertWalTableNotSuspended("c");
            try (TableReader reader = engine.getReader(engine.verifyTableName("c"))) {
                final int idx = reader.getMetadata().getColumnIndex("exch");
                Assert.assertTrue("exch must be indexed after ADD INDEX",
                        reader.getMetadata().isColumnIndexed(idx));
            }
        });
    }

    /**
     * SP2 (2026-08-18): DROP INDEX is no longer gated. Asserts the round trip -- add then drop -- so the
     * flag has to actually move in BOTH directions rather than merely not throwing.
     */
    @Test
    public void testDropIndexRoundTripsWithAddIndex() throws Exception {
        assertMemoryLeak(() -> {
            createRoutedTwoCellTable("c");
            final TableToken token = engine.verifyTableName("c");
            execute("alter table c alter column exch add index");
            drainWalQueue();
            assertWalTableNotSuspended("c");
            try (TableReader reader = engine.getReader(token)) {
                Assert.assertTrue("exch must be indexed after ADD INDEX",
                        reader.getMetadata().isColumnIndexed(reader.getMetadata().getColumnIndex("exch")));
            }
            execute("alter table c alter column exch drop index");
            drainWalQueue();
            assertWalTableNotSuspended("c");
            try (TableReader reader = engine.getReader(token)) {
                Assert.assertFalse("exch must NOT be indexed after DROP INDEX",
                        reader.getMetadata().isColumnIndexed(reader.getMetadata().getColumnIndex("exch")));
            }
        });
    }

    /**
     * Plan 4b feature-gate sweep. {@code O3PartitionJob#getDedupRowsWithAdditionalKeys} (reached
     * whenever the upsert-key list has any column besides the designated timestamp) resolves
     * per-partition columnTop/nameTxn via the same cellKey-0-only lookups this whole sweep rejects
     * elsewhere -- confirmed reachable (this exact CREATE statement was NOT rejected before this
     * gate was added). Gated unconditionally at CREATE time.
     */
    @Test
    public void testCreateCompositeWithDedupUpsertKeysGated() throws Exception {
        assertMemoryLeak(() -> {
            try {
                execute("create table c (ts timestamp, exch symbol, px double) timestamp(ts) " +
                        "partition by day, exch wal dedup upsert keys(ts, exch)");
                Assert.fail("expected composite + DEDUP UPSERT KEYS(ts, exch) to be rejected at CREATE time");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "composite partitioning does not yet support DEDUP UPSERT KEYS");
            }
        });
    }

    /**
     * Same gate as {@link #testCreateCompositeWithDedupUpsertKeysGated()}, but for the
     * timestamp-only upsert-key shape ({@code DEDUP UPSERT KEYS(ts)}, no additional columns). This
     * narrower shape does NOT reach the confirmed-unsafe {@code getDedupRowsWithAdditionalKeys} (it
     * takes the plain {@code Vect.mergeDedupTimestampWithLongIndexAsc} path instead), but the gate is
     * intentionally unconditional -- DEDUP's broader WAL-commit-reconciliation/symbol-remap machinery
     * is not yet audited for composite either, so the whole feature is rejected rather than carving
     * out a narrower "safe" subset. This test proves the gate condition is "any dedup key", not
     * "more than one dedup key".
     */
    @Test
    public void testCreateCompositeWithTimestampOnlyDedupGated() throws Exception {
        assertMemoryLeak(() -> {
            try {
                execute("create table c (ts timestamp, exch symbol, px double) timestamp(ts) " +
                        "partition by day, exch wal dedup upsert keys(ts)");
                Assert.fail("expected composite + DEDUP UPSERT KEYS(ts) to be rejected at CREATE time");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "composite partitioning does not yet support DEDUP UPSERT KEYS");
            }
        });
    }

    /**
     * Plan 4b feature-gate sweep. The CREATE-time guard ({@link #testCreateCompositeWithDedupUpsertKeysGated()})
     * stops a composite table from ever being BORN with dedup keys, but {@code ALTER TABLE ... DEDUP
     * ENABLE UPSERT KEYS(...)} is a second, independent SQL path that can attach dedup keys to an
     * already-existing composite table. Must be rejected too, synchronously (validated at compile
     * time, before any AlterOperation is built/enqueued).
     */
    @Test
    public void testAlterTableDedupEnableGated() throws Exception {
        assertMemoryLeak(() -> {
            createRoutedTwoCellTable("c");
            try {
                execute("alter table c dedup enable upsert keys(ts, exch)");
                Assert.fail("expected ALTER TABLE ... DEDUP ENABLE on a composite table to be rejected");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "composite partitioning does not yet support DEDUP UPSERT KEYS");
            }
            assertWalTableNotSuspended("c");
        });
    }

    /**
     * Plan 4b feature-gate sweep. {@code TableWriter#removeColumn}'s file purge
     * ({@code removeColumnFiles} -&gt; {@code PurgingOperator}/{@code ColumnPurgeOperator}) resolves
     * per-cell columnNameTxn and physical paths via cellKey-0-only/bare-path lookups with zero
     * composite awareness anywhere in either purge class -- confirmed reachable (this exact DROP
     * COLUMN was NOT rejected before this gate was added; it silently leaked the dropped column's
     * per-cell files instead).
     */
    @Test
    public void testDropColumnGated() throws Exception {
        assertMemoryLeak(() -> {
            createRoutedTwoCellTable("c");
            assertCompositeGateFires(
                    "alter table c drop column px",
                    "c",
                    "composite partitioning does not yet support DROP COLUMN");
        });
    }

    /**
     * Plan 4b feature-gate sweep. {@code TableWriter#renameColumn}'s
     * {@code hardLinkAndPurgeColumnFiles} resolves both the old-name source path (bare 5-arg
     * {@code setPathForNativePartition}) and the columnNameTxn to link (cellKey-0-only 2-arg
     * {@code ColumnVersionWriter} lookups) cell-blind, AFTER the new name is already durably
     * committed to metadata -- a worse failure shape than most gates in this sweep (a partial
     * metadata-vs-files split, not just a clean rejection). Confirmed reachable.
     */
    @Test
    public void testRenameColumnGated() throws Exception {
        assertMemoryLeak(() -> {
            createRoutedTwoCellTable("c");
            assertCompositeGateFires(
                    "alter table c rename column px to px2",
                    "c",
                    "composite partitioning does not yet support RENAME COLUMN");
        });
    }

    /**
     * Plan 4b feature-gate sweep. Unlike every other gate in this class, this one is reached by an
     * ORDINARY INSERT, not a discrete DDL command: {@code TableWriter#sealPostingIndexForPartition}
     * (run after every O3 commit that touches an indexed partition, for any table with a POSTING
     * index -- {@code TYPE POSTING} is a normal, documented feature, not an edge case) resolves the
     * partition's nameTxn via the cellKey-0-only {@code TxReader#getPartitionNameTxnByPartitionTimestamp}
     * wrapper, then (for the common, non-PARQUET case) builds the on-disk path via
     * {@code TableWriter#setStateForTimestamp}'s own bare 5-arg {@code setPathForNativePartition} --
     * cell-blind either way, and confirmed reachable simply by inserting into a routed 2-cell day.
     */
    @Test
    public void testPostingIndexSealGated() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table c (ts timestamp, exch symbol index type posting, px double) timestamp(ts) partition by day, exch wal");
            assertCompositeGateFires(
                    "insert into c values ('2020-01-01T00:00:00.000000Z','A',1.0), ('2020-01-01T12:00:00.000000Z','B',1.5)",
                    "c",
                    "composite partitioning does not yet support a POSTING index seal");
        });
    }

    @Test
    public void testReindexTableGated() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table c (ts timestamp, exch symbol index, px double) timestamp(ts) partition by day, exch wal");
            execute("insert into c values " +
                    "('2020-01-01T00:00:00.000000Z','A',1.0), ('2020-01-01T12:00:00.000000Z','B',1.5)");
            drainWalQueue();
            assertWalTableNotSuspended("c");
            // REINDEX TABLE is validated synchronously at the SQL layer (it never opens a TableWriter),
            // so this always throws directly from execute(), never via WAL suspension. The gate fires
            // before the mandatory LOCK EXCLUSIVE clause is even checked, but the full, well-formed
            // statement is used here anyway for rigor.
            try {
                execute("reindex table c lock exclusive");
                Assert.fail("expected REINDEX TABLE to be rejected on a composite table");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "composite partitioning does not yet support REINDEX TABLE");
            }
        });
    }

    // ------------------------------------------------------------------------------------------
    // SUPPORTED: these must keep working on a routed composite table -- this is the other half of
    // the convergence proof (the boundary is precise, not a blanket "composite tables are broken").
    // ------------------------------------------------------------------------------------------

    @Test
    public void testTruncateStillWorks() throws Exception {
        assertMemoryLeak(() -> {
            createRoutedTwoCellTable("c");
            execute("truncate table c");
            drainWalQueue();
            assertWalTableNotSuspended("c");
            engine.releaseInactive();
            assertQuery("select count() from c").noLeakCheck().noRandomAccess().expectSize().returns("count\n0\n");

            // Re-insert after truncate must not collide with anything left behind by the truncate.
            execute("insert into c values ('2020-01-05T00:00:00.000000Z','A',9.0)");
            drainWalQueue();
            assertWalTableNotSuspended("c");
            engine.releaseInactive();
            assertQuery("select count() from c").noLeakCheck().noRandomAccess().expectSize().returns("count\n1\n");
        });
    }

    /**
     * Plan 4b feature-gate sweep. Unlike every other gate in this class, the automatic O3
     * split-fragment squash ({@code TableWriter#squashSplitPartitions}, reached from
     * {@code housekeep()} after ordinary commits, once a cell's partition has split "too many"
     * times) is fixed by SKIPPING rather than throwing -- it is background housekeeping, not a
     * discrete DDL a user can avoid, and skipping causes no wrong answers (each split fragment
     * remains an independently valid, fully queryable physical partition). This test forces the
     * split threshold down to the minimum so an ordinary out-of-order commit into one cell of a
     * routed composite table genuinely triggers a real split, then asserts the table survives
     * {@code housekeep()}'s follow-on squash attempt with no corruption, no crash, and no
     * suspension -- proving the skip is effective, not just that nothing happened to trigger it.
     */
    @Test
    public void testAutomaticO3SplitOnCompositeTableDoesNotCorrupt() throws Exception {
        setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);
        assertMemoryLeak(() -> {
            execute("create table c (ts timestamp, exch symbol, px double) timestamp(ts) partition by day, exch wal");
            // A large in-order batch for cell A on day1 (the "prefix" a later small O3 write will
            // split off), plus one row for cell B on day1 -- the routed 2-cell shape.
            execute("insert into c select timestamp_sequence('2020-01-01T00:00:00.000000Z', 40000L), 'A', x::double from long_sequence(2000)");
            execute("insert into c values ('2020-01-01T23:59:59.000000Z','B',9999.0)");
            drainWalQueue();
            assertWalTableNotSuspended("c");
            engine.releaseInactive();
            assertQuery("select count() from c").noLeakCheck().noRandomAccess().expectSize().returns("count\n2001\n");

            // One small out-of-order row landing well inside cell A's already-committed range --
            // a large untouched prefix + a tiny O3 merge is exactly the shape that triggers a real
            // partition SPLIT (TableWriter#getPartitionO3SplitThreshold(), forced to ~1 row above).
            execute("insert into c values ('2020-01-01T00:01:00.000000Z','A',-1.0)");
            drainWalQueue();
            assertWalTableNotSuspended("c");

            // A further commit (any commit) drives housekeep() again, exercising the squash-skip
            // path a second time on an already-split composite table.
            execute("insert into c values ('2020-01-03T00:00:00.000000Z','A',3.0)");
            drainWalQueue();
            assertWalTableNotSuspended("c");

            engine.releaseInactive();
            assertQuery("select count() from c").noLeakCheck().noRandomAccess().expectSize().returns("count\n2003\n");
            // The O3 row's timestamp (2020-01-01T00:01:00.000000Z) coincides exactly with the
            // already-committed row at x=1501 (row index 1500 * 40000us = 60s past midnight) -- with
            // no DEDUP configured, both rows legitimately survive (same timestamp, distinct rows);
            // seeing BOTH back, in full, is itself further evidence the split+merge preserved every
            // row rather than silently dropping/overwriting one.
            assertQuery("select px from c where ts = '2020-01-01T00:01:00.000000Z' and exch = 'A'")
                    .noLeakCheck().returns("px\n1501.0\n-1.0\n");

            // SP1E (2026-08-18): this assertion INVERTED, and the inversion is the point. It used to
            // require MORE than 3 raw entries, as evidence that a real physical split had happened and
            // been left alone -- the squash-skip behaviour this test was written for. Composite squash
            // is now implemented for mid-table day groups, and the third insert above (2020-01-03) is
            // exactly what pushes day1 off the active tail, so day1's fragment is now MERGED BACK into
            // its cell and the count legitimately returns to 3: cell A + cell B on day1, plus day3.
            // Row-level evidence that nothing was lost is asserted above (2003 rows, and both rows at
            // the colliding timestamp). That a split physically occurs at all is pinned separately by
            // CompositeSquashTest#testCompositeSplitProducesACellStructuredFragment.
            engine.releaseInactive();
            printSql("select count() from table_partitions('c')");
            TestUtils.assertContains(sink, "count\n3\n");
        });
    }

    @Test
    public void testVacuumTableStillWorks() throws Exception {
        assertMemoryLeak(() -> {
            createRoutedTwoCellTable("c");
            // Previously used "alter table c drop column px" here to generate a column version for
            // VACUUM to (not) reconcile -- DROP COLUMN is now itself gated for composite (see
            // testDropColumnGated), so that setup shape is no longer SQL-reachable at all. VACUUM's
            // own safety (a no-op walk, per Plan 4a's sweep) does not depend on there being anything
            // to reconcile, so this simply proves VACUUM TABLE still runs cleanly on a routed
            // composite table with no setup beyond ordinary INSERT.
            execute("vacuum table c");

            engine.releaseInactive();
            assertQuery("select count() from c").noLeakCheck().noRandomAccess().expectSize().returns("count\n3\n");
            assertQuery("select ts, exch from c order by ts, exch").noLeakCheck().timestamp("ts").expectSize().returns(
                    "ts\texch\n" +
                            "2020-01-01T00:00:00.000000Z\tA\n" +
                            "2020-01-01T12:00:00.000000Z\tB\n" +
                            "2020-01-02T00:00:00.000000Z\tA\n");
        });
    }

    @Test
    public void testInsertNewCellsAndPlainSelectStillWork() throws Exception {
        assertMemoryLeak(() -> {
            createRoutedTwoCellTable("c");
            assertWalTableNotSuspended("c");
            engine.releaseInactive();
            // Plain, unfiltered SELECT (no ts-range filter) over a routed composite table.
            assertQuery("select count() from c").noLeakCheck().noRandomAccess().expectSize().returns("count\n3\n");
            assertQuery("select exch, count() from c order by exch").noLeakCheck().expectSize().returns(
                    "exch\tcount\nA\t2\nB\t1\n");

            // A brand-new cell on a brand-new day -- the well-supported write-routing path (Plan 4a
            // Tasks 4/5), unaffected by any of this sweep's gates.
            execute("insert into c values ('2020-01-03T00:00:00.000000Z','C',3.0)");
            drainWalQueue();
            assertWalTableNotSuspended("c");
            engine.releaseInactive();
            assertQuery("select count() from c").noLeakCheck().noRandomAccess().expectSize().returns("count\n4\n");
        });
    }

    // ------------------------------------------------------------------------------------------
    // NEGATIVE CONTROL: a PLAIN (non-composite) table must be completely unaffected by every gate
    // added in this sweep.
    // ------------------------------------------------------------------------------------------

    @Test
    public void testPlainTableUnaffectedByAllGates() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table p (ts timestamp, exch symbol index, note varchar, px double) timestamp(ts) partition by day wal");
            execute("insert into p values " +
                    "('2020-01-01T00:00:00.000000Z','A','1',1.0), ('2020-01-02T00:00:00.000000Z','A','2',2.0), " +
                    "('2020-01-03T00:00:00.000000Z','A','3',3.0)");
            drainWalQueue();
            assertWalTableNotSuspended("p");

            execute("alter table p drop partition list '2020-01-01'");
            drainWalQueue();
            assertWalTableNotSuspended("p");

            // FORCE DROP PARTITION bypasses WAL entirely (AlterOperation#isForceWalBypass), applied
            // synchronously -- no drainWalQueue needed. Leaves only 2020-01-03 attached.
            execute("alter table p force drop partition list '2020-01-02'");
            assertWalTableNotSuspended("p");

            execute("alter table p convert partition to parquet list '2020-01-03'");
            drainWalQueue();
            assertWalTableNotSuspended("p");

            execute("alter table p convert partition to native list '2020-01-03'");
            drainWalQueue();
            assertWalTableNotSuspended("p");

            execute("alter table p squash partitions");
            drainWalQueue();
            assertWalTableNotSuspended("p");

            // Only one partition remains (2020-01-03) at this point -- enforceTtl's own
            // getPartitionCount() < 2 short-circuit makes this a guaranteed no-op eviction-wise,
            // regardless of the TTL duration chosen; this only proves SET TTL itself is unaffected.
            execute("alter table p set ttl 1 day");
            drainWalQueue();
            assertWalTableNotSuspended("p");

            execute("alter table p alter column note type symbol");
            drainWalQueue();
            assertWalTableNotSuspended("p");

            execute("update p set px = px + 1");
            drainWalQueue();
            assertWalTableNotSuspended("p");

            execute("alter table p alter column exch drop index");
            drainWalQueue();
            assertWalTableNotSuspended("p");

            execute("alter table p alter column exch add index");
            drainWalQueue();
            assertWalTableNotSuspended("p");

            // REINDEX TABLE takes an exclusive filesystem lock on the table directory (RebuildColumnBase
            // #lock) -- it needs no pooled writer/reader holding the table, matching the established
            // idiom in SqlCompilerImplTest's own REINDEX tests.
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            execute("reindex table p lock exclusive");

            execute("truncate table p");
            drainWalQueue();
            assertWalTableNotSuspended("p");

            // Plan 4b feature-gate sweep additions: DROP COLUMN, RENAME COLUMN, and DEDUP
            // ENABLE/DISABLE must all remain completely unaffected on a plain table.
            execute("alter table p drop column px");
            drainWalQueue();
            assertWalTableNotSuspended("p");

            execute("alter table p rename column note to note2");
            drainWalQueue();
            assertWalTableNotSuspended("p");

            execute("alter table p dedup enable upsert keys(ts)");
            drainWalQueue();
            assertWalTableNotSuspended("p");

            execute("alter table p dedup disable");
            drainWalQueue();
            assertWalTableNotSuspended("p");

            execute("vacuum table p");
        });
    }

    /**
     * Plan 4b feature-gate sweep. {@code CREATE TABLE ... DEDUP UPSERT KEYS(...)} on a PLAIN
     * (non-composite) table must be completely unaffected by {@link
     * #testCreateCompositeWithDedupUpsertKeysGated()}'s new CREATE-time guard.
     */
    @Test
    public void testPlainTableCreateWithDedupUnaffected() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table p (ts timestamp, exch symbol, px double) timestamp(ts) partition by day wal dedup upsert keys(ts, exch)");
            execute("insert into p values ('2020-01-01T00:00:00.000000Z','A',1.0), ('2020-01-01T00:00:00.000000Z','A',2.0)");
            drainWalQueue();
            assertWalTableNotSuspended("p");
            engine.releaseInactive();
            // The duplicate (ts, exch) pair must have been deduped down to the last-committed value --
            // proves DEDUP is not just accepted at CREATE time but genuinely still active/functional.
            assertQuery("select count() from p").noLeakCheck().noRandomAccess().expectSize().returns("count\n1\n");
            assertQuery("select px from p").noLeakCheck().expectSize().returns("px\n2.0\n");
        });
    }

    /**
     * Plan 4b feature-gate sweep. A POSTING index on a PLAIN (non-composite) table must be completely
     * unaffected by {@link #testPostingIndexSealGated()}'s new gate: ordinary inserts must keep
     * sealing the index correctly, proven here by an indexed-column filter returning the right rows.
     */
    @Test
    public void testPlainTablePostingIndexSealUnaffected() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table p (ts timestamp, exch symbol index type posting, px double) timestamp(ts) partition by day wal");
            execute("insert into p values ('2020-01-01T00:00:00.000000Z','A',1.0), ('2020-01-01T00:00:01.000000Z','B',1.5)");
            drainWalQueue();
            assertWalTableNotSuspended("p");
            engine.releaseInactive();
            assertQuery("select count() from p where exch = 'A'").noLeakCheck().noRandomAccess().expectSize().returns("count\n1\n");
        });
    }

    // ------------------------------------------------------------------------------------------
    // NEGATIVE CONTROL: a composite table that has NEVER routed a single row (no INSERT ever --
    // "dormant" in the DDL-safety sense, NOT to be confused with the narrower, legacy-data-specific
    // isDormantWithPreexistingData()) must NOT be gated by any of the ops above. None of those gates'
    // stated hazards (cell-blind purge/rename/rebuild of PHYSICAL PER-CELL FILES) can exist yet: no
    // row has ever been routed, so no per-cell directory has ever been created. This is the
    // regression lock for the bug found via ShowCreateTableTest's 3 pre-existing failures
    // (testShowCreateCompositeAfterDropLowerIndexDimensionColumn and 2 siblings) and fixed by
    // TableWriter#isRoutedComposite() (see its own doc for the full reasoning).
    // ------------------------------------------------------------------------------------------

    @Test
    public void testGatesDoNotFireOnNeverRoutedEmptyCompositeTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table c (ts timestamp, exch symbol index, note varchar, price double, qty int) " +
                    "timestamp(ts) partition by day, exch wal");
            // Not a single row has ever been inserted: registry is empty AND maxTimestamp is
            // MIN_VALUE, i.e. dimCount>0 but genuinely never-routed -- the "state A" case
            // isRoutedComposite() must treat as safe (unlike the old !isDormantWithPreexistingData()
            // gate, which requires PREEXISTING DATA to read dormant and so stayed armed here).

            execute("alter table c set ttl 1 day");
            drainWalQueue();
            assertWalTableNotSuspended("c");

            // Disable TTL again before proceeding: enforceTtl() is ALSO invoked automatically from
            // every commit's housekeep() (TableWriter#commitWalInsertTransactions), not just
            // synchronously after SET TTL. Left armed, it would correctly (and expectedly) re-fire
            // once the table's first real row below transitions it from never-routed to routed --
            // that later gating is CORRECT (matches CompositeUnsupportedOpsTest#testSetTtlGated), not
            // a bug, but it would defeat this test's specific point, which is proving the NEVER-ROUTED
            // state itself is never gated. ttl==0 short-circuits enforceTtl() before it ever reaches
            // isRoutedComposite(), so this only re-proves SET TTL itself (both arming and disarming)
            // is ungated here, and keeps the rest of this test isolated from that separate concern.
            execute("alter table c set ttl 0 hours");
            drainWalQueue();
            assertWalTableNotSuspended("c");

            // exch already has an index (declared at CREATE time, matching testDropIndexGated's own
            // established workaround) -- drop it, then add it back, round-tripping the same column
            // used as the composite dimension source (matching testAddIndexGated's own precedent that
            // indexing the dimension column itself is unrestricted).
            execute("alter table c alter column exch drop index");
            drainWalQueue();
            assertWalTableNotSuspended("c");

            execute("alter table c alter column exch add index");
            drainWalQueue();
            assertWalTableNotSuspended("c");

            // STRING, not SYMBOL: converting a column TO symbol type hits a separate, legitimate,
            // always-on guard (TableWriter#changeColumnType, "ALTER COLUMN TYPE SYMBOL is not yet
            // supported on composite-partitioned tables") -- an orthogonal symbol-interner-slot-ordering
            // hazard, unconditional on dimensionCount>0 alone (not gated by routed-state at all, so it
            // fires even here) and correctly out of this fix's scope. STRING exercises the OTHER,
            // isRoutedComposite()-gated "ALTER COLUMN TYPE is not yet cell-aware for ANY target type"
            // guard just above it in the same method, without tripping the unrelated one.
            execute("alter table c alter column note type string");
            drainWalQueue();
            assertWalTableNotSuspended("c");

            execute("alter table c rename column qty to qty2");
            drainWalQueue();
            assertWalTableNotSuspended("c");

            execute("alter table c drop column price");
            drainWalQueue();
            assertWalTableNotSuspended("c");

            // The table must still be genuinely usable afterward: insert real data through the
            // now-altered column set and confirm it routes normally -- proves none of the above
            // silently corrupted metadata, the partition spec, or the (still-empty) cell registry.
            execute("insert into c (ts, exch, note, qty2) values ('2020-01-01T00:00:00.000000Z','A','n',7)");
            drainWalQueue();
            assertWalTableNotSuspended("c");
            engine.releaseInactive();
            assertQuery("select count() from c").noLeakCheck().noRandomAccess().expectSize().returns("count\n1\n");
            assertQuery("select ts, exch, note, qty2 from c").noLeakCheck().timestamp("ts").expectSize().returns(
                    "ts\texch\tnote\tqty2\n2020-01-01T00:00:00.000000Z\tA\tn\t7\n");
        });
    }

    /**
     * Builds a routed composite table {@code tableName} ({@code partition by day, exch}): day1 gets
     * BOTH {@code exch='A'} and {@code exch='B'} (two real cells sharing one day), day2 gets a third
     * row -- all in ONE commit, the safe multi-cell shape {@code CompositeRoutingTest}'s own
     * acceptance tests already prove routes correctly (not the separately-guarded "extend an
     * already-populated cell" shape). 3 rows total.
     */
    private void createRoutedTwoCellTable(String tableName) throws Exception {
        execute("create table " + tableName + " (ts timestamp, exch symbol, px double) timestamp(ts) partition by day, exch wal");
        execute("insert into " + tableName + " values " +
                "('2020-01-01T00:00:00.000000Z','A',1.0), ('2020-01-01T12:00:00.000000Z','B',1.5), " +
                "('2020-01-02T00:00:00.000000Z','A',2.0)");
        drainWalQueue();
        assertWalTableNotSuspended(tableName);
    }

    /**
     * Executes {@code sql} against a routed composite table and asserts the new gate's message fires
     * -- either synchronously (REINDEX TABLE, and any op the SQL layer can reject before ever
     * touching a TableWriter) or, far more commonly for a WAL table's ALTER/UPDATE operations, via
     * WAL suspension after {@code drainWalQueue()} (mirrors {@code CompositeRoutingTest}'s own
     * {@code assertWalTableSuspendedWithMessage} idiom). Handles both without the caller needing to
     * know which applies to a given op.
     */
    private void assertCompositeGateFires(String sql, String tableName, String expectedMessageSubstring) throws Exception {
        boolean threwSynchronously;
        try {
            execute(sql);
            threwSynchronously = false;
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), expectedMessageSubstring);
            threwSynchronously = true;
        } catch (CairoException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), expectedMessageSubstring);
            threwSynchronously = true;
        }
        if (!threwSynchronously) {
            drainWalQueue();
            Assert.assertTrue(
                    tableName + " must be suspended after a not-yet-supported composite DDL op (" + sql + ')',
                    engine.getTableSequencerAPI().isSuspended(engine.verifyTableName(tableName)));
            printSql("select errorMessage from wal_tables() where name = '" + tableName + "'");
            TestUtils.assertContains(sink, expectedMessageSubstring);
        }
    }

    private void assertWalTableNotSuspended(String tableName) {
        Assert.assertFalse(
                tableName + " must not be suspended",
                engine.getTableSequencerAPI().isSuspended(engine.verifyTableName(tableName)));
    }
}
