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

import io.questdb.cairo.CairoException;
import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
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
 * already prove routes correctly) before its gate was added -- see
 * {@code .superpowers/sdd/plan4a-ddl-gate-sweep-report.md} for the full op-by-op evidence, including
 * a live-reproduced INFINITE LOOP for DROP PARTITION (not just a clean crash).
 */
public class CompositeUnsupportedOpsTest extends AbstractCairoTest {

    // ------------------------------------------------------------------------------------------
    // GATED: every op below must throw "composite partitioning does not yet support <OP>" on a
    // real routed composite table.
    // ------------------------------------------------------------------------------------------

    @Test
    public void testDropPartitionGated() throws Exception {
        assertMemoryLeak(() -> {
            createRoutedTwoCellTable("c");
            assertCompositeGateFires(
                    "alter table c drop partition list '2020-01-01'",
                    "c",
                    "composite partitioning does not yet support DROP PARTITION");
        });
    }

    @Test
    public void testDetachPartitionGated() throws Exception {
        assertMemoryLeak(() -> {
            createRoutedTwoCellTable("c");
            assertCompositeGateFires(
                    "alter table c detach partition list '2020-01-01'",
                    "c",
                    "composite partitioning does not yet support DETACH PARTITION");
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

    @Test
    public void testSquashPartitionsGated() throws Exception {
        assertMemoryLeak(() -> {
            createRoutedTwoCellTable("c");
            assertCompositeGateFires(
                    "alter table c squash partitions",
                    "c",
                    "composite partitioning does not yet support SQUASH PARTITIONS");
        });
    }

    /**
     * Plan 4b Task 1b. FORCE DROP PARTITION is the ungated sibling of (plain) DROP PARTITION --
     * {@code AlterOperation#isForceWalBypass} routes it around the WAL entirely (applied synchronously,
     * bypassing the sequencer), so it needed its own dedicated gate; {@link #testDropPartitionGated()}
     * alone never exercised it.
     */
    @Test
    public void testForceDropPartitionGated() throws Exception {
        assertMemoryLeak(() -> {
            createRoutedTwoCellTable("c");
            assertCompositeGateFires(
                    "alter table c force drop partition list '2020-01-01'",
                    "c",
                    "composite partitioning does not yet support FORCE DROP PARTITION");
        });
    }

    /**
     * Plan 4b Task 1b. TTL eviction shares {@code dropPartitionByExactTimestamp}'s cell-blind selection
     * chain with (plain) DROP PARTITION -- the same infinite-loop/sibling-deletion risk {@link
     * #testDropPartitionGated()}'s own gate was added for, but reached via {@code ALTER TABLE ... SET TTL}
     * instead, which was left ungated until now.
     */
    @Test
    public void testSetTtlGated() throws Exception {
        assertMemoryLeak(() -> {
            createRoutedTwoCellTable("c");
            assertCompositeGateFires(
                    "alter table c set ttl 1 day",
                    "c",
                    "composite partitioning does not yet support TTL-based partition eviction");
        });
    }

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
                    "composite partitioning does not yet support UPDATE");
        });
    }

    @Test
    public void testAddIndexGated() throws Exception {
        assertMemoryLeak(() -> {
            createRoutedTwoCellTable("c");
            assertCompositeGateFires(
                    "alter table c alter column exch add index",
                    "c",
                    "composite partitioning does not yet support ADD INDEX");
        });
    }

    @Test
    public void testDropIndexGated() throws Exception {
        assertMemoryLeak(() -> {
            // ADD INDEX is itself gated (retroactive rebuild is unsafe), so to reach a composite
            // table with an actual index to drop, the index must be declared AT CREATE time -- the
            // one ADD-INDEX-shaped operation this sweep did NOT find broken (nothing to retroactively
            // rebuild: the table is empty at CREATE), matching CompositeEndToEndTest's own established
            // workaround for the same constraint (N2).
            execute("create table c (ts timestamp, exch symbol index, px double) timestamp(ts) partition by day, exch wal");
            execute("insert into c values " +
                    "('2020-01-01T00:00:00.000000Z','A',1.0), ('2020-01-01T12:00:00.000000Z','B',1.5)");
            drainWalQueue();
            assertWalTableNotSuspended("c");
            assertCompositeGateFires(
                    "alter table c alter column exch drop index",
                    "c",
                    "composite partitioning does not yet support DROP INDEX");
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

    @Test
    public void testVacuumTableStillWorks() throws Exception {
        assertMemoryLeak(() -> {
            createRoutedTwoCellTable("c");
            execute("alter table c drop column px"); // generates a column version to (not) vacuum
            drainWalQueue();
            assertWalTableNotSuspended("c");

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

            execute("vacuum table p");
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
