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
import io.questdb.cairo.ColumnPurgeJob;
import io.questdb.cairo.O3PartitionPurgeJob;
import io.questdb.cairo.TableReader;
import io.questdb.std.Misc;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Plan 3b (composite partitioning), Task 5 -- CAPSTONE: a broad end-to-end proof that a DORMANT
 * composite table (partitioned by time + a symbol dimension; the un-routed write path in this phase
 * only ever produces cellKey 0 -- real (ts, cellKey) multi-cell write-routing is Plan 4) behaves
 * IDENTICALLY to an equivalent plain (time-only) table across every engine path that reads the
 * {@code _txn} partition table -- especially the six paths Plan 3b's Task 10 investigation found
 * misreading a composite {@code _txn} before the self-describing stride marker (Tasks 1-4) existed:
 * {@code table_storage()}, {@code MetadataCache.hydrateTableStartup}, {@link O3PartitionPurgeJob},
 * {@link ColumnPurgeJob}/{@code ColumnPurgeOperator}, {@code RebuildColumnBase}/{@code IndexBuilder},
 * and {@code TableSnapshotRestore}.
 * <p>
 * Two twin tables are built with byte-for-byte identical rows across 5 day partitions (10 rows): {@code
 * c} ({@code partition by day, exchange} -- composite, stride-8 {@code _txn}) and {@code p} ({@code
 * partition by day} -- plain, stride-4 {@code _txn}). Every assertion below compares {@code c} against
 * {@code p} directly, mostly via {@link #assertSqlCursors}, rather than a hardcoded expected row set --
 * pinning the ONE property this task cares about (composite == plain) without coupling the test to
 * incidental formatting.
 * <p>
 * <b>Whole-branch review (Plan 4a) update:</b> this class originally built {@code c} WITHOUT {@code WAL}
 * and with 2 distinct {@code exchange} values (A/B), so every row landed at cellKey 0 via the direct,
 * non-WAL append path -- genuinely DORMANT, never actually routed. Finding I1 now rejects a non-WAL
 * composite table at CREATE (its direct append path silently never routes), so {@code c} is now WAL --
 * which means it DOES route for real from its first commit. Every row now uses a SINGLE {@code exchange}
 * value ({@code 'A'}) so {@code c}'s on-disk shape stays exactly one physical partition per day (matching
 * this class's original hardcoded partition/row counts) while still being genuinely, actually routed
 * (registry non-empty), rather than trying to preserve a "dormant" state I1 makes unreachable going
 * forward. Findings C2+C3 separately mean {@code TableSnapshotRestore} now REFUSES to restore a
 * genuinely-routed composite table (assertion 10 below is restructured accordingly -- see that test's own
 * javadoc); a brand-new day whose only cell isn't cellKey 0 is separately covered by finding C1's own
 * dedicated regression test in {@code CompositeRoutingTest}, not here.
 * <p>
 * Split into {@code @Test} methods by concern so a failure localizes without re-running the whole class:
 * queries (1-3), catalogue/introspection (4-6), mutating DDL (7-8), O3 + partition purge (9), and
 * checkpoint/snapshot restore (10, now two tests: refused-for-live and allowed-for-dormant).
 */
public class CompositeEndToEndTest extends AbstractCairoTest {
    private static O3PartitionPurgeJob purgeJob;

    @AfterClass
    public static void tearDownPurgeJob() {
        purgeJob = Misc.free(purgeJob);
    }

    @BeforeClass
    public static void setUpStatic() throws Exception {
        AbstractCairoTest.setUpStatic();
        purgeJob = new O3PartitionPurgeJob(engine, 1);
    }

    /**
     * Brief assertions 1-3: full timestamp-ordered scan, {@code WHERE ts in '<day>'} time pruning,
     * {@code count()}, and {@code LATEST ON ts PARTITION BY exchange} -- a composite table's query
     * results must be identical, row for row, to an equivalent plain table's.
     */
    @Test
    public void testQueriesMatchPlainEquivalent() throws Exception {
        assertMemoryLeak(() -> {
            createAndPopulateTwins();
            engine.releaseInactive(); // cold reopen -- no pooled reader may mask a fresh self-detect

            // 1. Full timestamp-ordered scan.
            assertSqlCursors("select ts, exchange, px from p order by ts", "select ts, exchange, px from c order by ts");

            // 2. WHERE ts in '<oneday>' time pruning: exactly that day's 2 rows, not zero/all.
            assertQuery("select count() from c where ts in '2020-01-03'")
                    .noLeakCheck().noRandomAccess().expectSize().returns("count\n2\n");
            assertSqlCursors(
                    "select ts, exchange, px from p where ts in '2020-01-03' order by ts",
                    "select ts, exchange, px from c where ts in '2020-01-03' order by ts");

            // 3. count() and LATEST ON ts PARTITION BY exchange.
            assertQuery("select count() from c").noLeakCheck().noRandomAccess().expectSize().returns("count\n10\n");
            assertSqlCursors("select count() from p", "select count() from c");
            assertSqlCursors(
                    "select ts, exchange, px from p latest on ts partition by exchange order by exchange",
                    "select ts, exchange, px from c latest on ts partition by exchange order by exchange");
        });
    }

    /**
     * Brief assertions 4-6: catalogue/introspection surfaces -- {@code SHOW CREATE TABLE}'s composite
     * clause round-trip (Plan 1 unbroken), {@code table_storage()}'s {@code partitionCount} (the
     * doubled-count site Plan 3b's marker fixes), and {@code table_partitions()}'s row count -- must
     * all read a composite table exactly like an equivalent plain table.
     */
    @Test
    public void testCatalogueMatchesPlainEquivalent() throws Exception {
        assertMemoryLeak(() -> {
            createAndPopulateTwins();
            engine.releaseInactive();

            // 4. SHOW CREATE TABLE still round-trips the composite partition-by clause (now WAL, per I1).
            assertQuery("show create table c").noLeakCheck().noRandomAccess().returns(
                    "ddl\n" +
                            "CREATE TABLE 'c' ( \n" +
                            "\tts TIMESTAMP,\n" +
                            "\texchange SYMBOL,\n" +
                            "\tpx DOUBLE\n" +
                            ") timestamp(ts) PARTITION BY DAY, exchange WAL;\n");

            // 5. table_storage() partitionCount -- the doubled-count site -- must equal p's (not 2x).
            assertSqlCursors(
                    "select partitionCount from table_storage() where tableName = 'p'",
                    "select partitionCount from table_storage() where tableName = 'c'");
            assertQuery("select partitionCount from table_storage() where tableName = 'c'")
                    .noLeakCheck().noRandomAccess().returns("partitionCount\n5\n");

            // 6. table_partitions() row count must equal p's.
            assertSqlCursors("select count() from table_partitions('p')", "select count() from table_partitions('c')");
            assertQuery("select count() from table_partitions('c')")
                    .noLeakCheck().noRandomAccess().expectSize().returns("count\n5\n");
        });
    }

    /**
     * Brief assertions 7-8: mutating DDL that must re-derive per-partition state across a composite
     * table's every day partition -- {@code ALTER TABLE ... ALTER COLUMN ... ADD INDEX} and {@code ADD
     * COLUMN} -- must leave the composite table exactly as correct as before.
     * <p>
     * <b>Plan 4b feature-gate sweep UPDATE (assertion 8):</b> this used to also DROP the added column
     * and drive its async column-purge cycle ({@link ColumnPurgeJob}/{@code ColumnPurgeOperator}, which
     * resolves the table's partitions through {@code _txn}) to completion. DROP COLUMN is now
     * unconditionally gated for a real composite table (its purge queue was confirmed cell-blind --
     * see {@code TableWriter#removeColumn}'s own gate comment and {@code
     * CompositeUnsupportedOpsTest#testDropColumnGated}), so assertion 8 now proves the gate fires
     * instead of driving a purge cycle that can no longer happen.
     * <p>
     * <b>NEW FINDING while porting assertion 7 to WAL for I1 (out of scope for this fix pass, NOT
     * fixed):</b> {@code ALTER TABLE ... ALTER COLUMN ... ADD INDEX} on a composite dimension column that
     * already has committed, genuinely-routed data throws {@code CairoException} "file does not exist"
     * for {@code <day>/<cell>/<column>.k} -- {@code RebuildColumnBase}/{@code IndexBuilder}'s retroactive
     * index-build walk resolves each EXISTING partition's path the bare, cell-blind way (like C1/the
     * DROP-active-partition finding above), so it writes the freshly-built {@code .k}/{@code .v} index
     * files directly under the bare day dir instead of the nested cell dir the column's own data (and
     * every read) actually lives in -- reproduces even with only ONE cell per day (not a multi-cell-
     * specific gap). This is the SAME "day-blind maintenance path" class as C1, a DIFFERENT call site,
     * not one of this pass's three assigned findings (C1/C2+C3/I1) -- flagged here, not fixed. The
     * ORIGINAL scenario (retroactively indexing an ALREADY-POPULATED composite table) was only ever
     * reachable because the pre-I1 table was dormant (bare day dirs, no nested cell dir to miss); it is
     * not reachable, safely, today. This assertion instead declares the index AT {@code CREATE TABLE}
     * time -- so every row is indexed incrementally as it is appended (the same path new commits always
     * use, unaffected by this gap) and {@code RebuildColumnBase}'s retroactive walk never runs at all --
     * on a dedicated table pair, still proving a composite table's indexed dimension column discriminates
     * correctly, just not via a retroactive {@code ADD INDEX} over pre-existing data.
     */
    @Test
    public void testMutationsMatchPlainEquivalent() throws Exception {
        assertMemoryLeak(() -> {
            createAndPopulateTwins();
            engine.releaseInactive();

            // 7. A dimension column indexed from CREATE (not retroactively added over already-populated
            // partitions -- see this method's own javadoc) must correctly discriminate rows by value,
            // matching an equivalent plain table.
            execute("create table ci (ts timestamp, exchange symbol index, px double) timestamp(ts) partition by day, exchange wal");
            execute("create table pi (ts timestamp, exchange symbol index, px double) timestamp(ts) partition by day");
            final String indexedRows = " values " +
                    "('2020-01-01T00:00:00.000000Z','A',1.0), ('2020-01-02T00:00:00.000000Z','A',2.0), " +
                    "('2020-01-03T00:00:00.000000Z','B',3.0)";
            execute("insert into ci" + indexedRows);
            execute("insert into pi" + indexedRows);
            drainWalQueue();
            assertSqlCursors(
                    "select ts, exchange, px from pi where exchange = 'A' order by ts",
                    "select ts, exchange, px from ci where exchange = 'A' order by ts");
            assertQuery("select count() from ci where exchange = 'A'")
                    .noLeakCheck().noRandomAccess().expectSize().returns("count\n2\n");

            // 8. ADD COLUMN + DROP COLUMN, then force a column-purge cycle to completion, then re-query.
            //
            // Plan 4b feature-gate sweep UPDATE: DROP COLUMN is now unconditionally gated for a real
            // (routed) composite table -- see TableWriter#removeColumn's own gate comment and
            // CompositeUnsupportedOpsTest#testDropColumnGated for the dedicated coverage (its
            // PurgingOperator/ColumnPurgeOperator purge queue was confirmed cell-blind: it silently
            // leaked a routed multi-cell day's dropped-column files rather than reclaiming them, with
            // ZERO composite awareness anywhere in either purge class). This assertion previously drove
            // DROP COLUMN's async ColumnPurgeJob cycle to completion on THIS composite table `c` -- that
            // shape is no longer reachable at all (the ALTER is rejected before any purge task is ever
            // queued), so this now proves the gate fires instead, and that `q` -- since the drop never
            // happened -- correctly remains part of `c`'s live schema. The general (non-composite)
            // ColumnPurgeJob drive-to-completion mechanism this assertion used to exercise remains
            // covered independently by ColumnPurgeJobTest; it does not depend on composite tables at all.
            execute("alter table c add column q double");
            drainWalQueue();
            Assert.assertFalse(
                    "c must not be suspended after ADD COLUMN",
                    engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("c")));

            execute("alter table c drop column q");
            drainWalQueue();
            Assert.assertTrue(
                    "c must be suspended after a not-yet-supported composite DROP COLUMN",
                    engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("c")));
            printSql("select errorMessage from wal_tables() where name = 'c'");
            TestUtils.assertContains(sink, "composite partitioning does not yet support DROP COLUMN");
            engine.getTableSequencerAPI().resumeTable(engine.verifyTableName("c"), 0);
            drainWalQueue();

            assertSqlCursors("select ts, exchange, px from p order by ts", "select ts, exchange, px from c order by ts");
            // Explicit metadata proof DROP COLUMN did NOT complete (the gate fired before any mutation):
            // q must still be present, one column ahead of the original 3-column shape shared with p.
            try (TableReader reader = getReader("c")) {
                Assert.assertTrue(reader.getMetadata().getColumnIndexQuiet("q") > -1);
                Assert.assertEquals(4, reader.getMetadata().getColumnCount());
            }
        });
    }

    /**
     * Brief assertion 9: an out-of-order (O3) insert schedules {@link O3PartitionPurgeJob} -- the job
     * that, pre-marker, misread a composite {@code _txn}'s attached-partitions region at the wrong
     * stride and could misidentify (and delete) a LIVE partition directory. After driving the purge job
     * to completion, EVERY partition and row must still be present -- no partition-directory loss -- and
     * the full scan must still equal {@code p}'s.
     * <p>
     * This test harness enables {@code CAIRO_O3_PARTITION_OVERWRITE_CONTROL_ENABLED} by default (unlike
     * production; see {@code Overrides}), so this also exercises Plan 3b Task 4's fix to the static
     * {@code TxReader#findPartitionRawIndex} (the one path {@code PartitionOverwriteControl} uses) live,
     * through real O3 SQL inserts -- not just the synthetic unit test that task added.
     * <p>
     * <b>Whole-branch review (Plan 4a) finding I1 update:</b> the ORIGINAL scenario inserted MORE rows
     * into day1 -- an EARLIER, ALREADY-COMMITTED day -- specifically to churn that partition's on-disk
     * name-txn VERSION and schedule the OLD version for purge. Once {@code c} is WAL (required by I1),
     * any later commit adding rows to an ALREADY-POPULATED (day, cellKey) pair is exactly Plan 4a Task
     * 5's own guarded "extend an already-populated cell" shape (day1's only cell, {@code exchange='A'},
     * was already populated by {@code createAndPopulateTwins}' commit) -- unavoidably, since churning an
     * EXISTING partition's version is definitionally re-writing an already-populated cell. This is the
     * SAME pre-existing, out-of-scope limitation documented on {@code CompositePartitionDdlTest
     * #testSquashNonFirstPartitionMatchesPlainEquivalent} -- not something this fix pass introduces or
     * can fix. The O3 insert below instead targets a BRAND-NEW, earlier day (2019-12-31) -- a genuinely
     * out-of-order backfill into a cell that has never existed before, Task 5's own proven-safe shape --
     * so this still exercises "O3 insert, then {@code O3PartitionPurgeJob} run, no partition/row loss"
     * end to end, just not the specific name-txn-churn-on-an-existing-partition mechanic the ORIGINAL
     * (dormant, non-WAL) version could reach. C1's OWN specific repro (a day whose only cell is NOT
     * cellKey 0) is separately, precisely covered by {@code CompositeRoutingTest
     * #testO3PartitionPurgeJobDoesNotDeleteNonCellZeroDay}.
     */
    @Test
    public void testO3InsertAndPurgeMatchPlainEquivalent() throws Exception {
        assertMemoryLeak(() -> {
            createAndPopulateTwins();

            // Out-of-order backfill into a BRAND-NEW day, earlier than every existing day -- a genuinely
            // new (day, cellKey) pair, not a re-write of an existing one (see class javadoc above).
            execute("insert into c values ('2019-12-31T06:00:00.000000Z','A',0.25), ('2019-12-31T18:00:00.000000Z','A',0.35)");
            execute("insert into p values ('2019-12-31T06:00:00.000000Z','A',0.25), ('2019-12-31T18:00:00.000000Z','A',0.35)");
            drainWalQueue();

            // Drive O3PartitionPurgeJob to completion (mirrors O3PartitionPurgeTest#runPartitionPurgeJobs).
            engine.releaseInactive(); // pooled readers/writers must release before purge can proceed
            purgeJob.drain(0);

            Assert.assertEquals("0 partition purge errors expected",
                    0, engine.getPartitionOverwriteControl().getErrorCount());

            // NO partition/row loss: every day's data is still present and matches p's row for row.
            assertSqlCursors("select ts, exchange, px from p order by ts", "select ts, exchange, px from c order by ts");
            assertQuery("select count() from c").noLeakCheck().noRandomAccess().expectSize().returns("count\n12\n");
            assertSqlCursors("select count() from p", "select count() from c");
            assertSqlCursors(
                    "select partitionCount from table_storage() where tableName = 'p'",
                    "select partitionCount from table_storage() where tableName = 'c'");
        });
    }

    /**
     * Brief assertion 10, whole-branch review (Plan 4a) findings C2+C3: a CHECKPOINT CREATE + restore
     * attempt against a genuinely-routed composite table must be REFUSED, loudly, instead of silently
     * corrupting or losing data. Mirrors {@code CheckpointTest#testCheckpointRestoreIndexNonPartitioned}'s
     * in-process create-then-recover idiom (change the configured snapshot instance id between create and
     * recover so {@code engine.checkpointRecover()} treats this as restoring onto a different install,
     * instead of no-op'ing because it looks like the same instance that made the checkpoint).
     * <p>
     * <b>Why this test changed:</b> the ORIGINAL version of this test (pre-I1) built {@code c} without
     * WAL, so every row landed at cellKey 0 via the direct append path -- genuinely dormant, registry
     * always empty -- and {@code TableSnapshotRestore} restored it byte-identically to plain, which was
     * safe precisely because there was nothing cell-aware to get wrong. Finding I1 makes that non-WAL
     * shape uncreatable going forward (see {@code CreateTableOperationBuilderImpl
     * #resolvePartitionSpec}), and a WAL composite table's first commit always routes for real (registry
     * non-empty) -- so there is no longer any ordinary way to reach the old "dormant, safe-to-restore"
     * state for a table with committed data. Findings C2 ({@code removePartitionDirsNotAttached} can
     * delete a live day when the table root holds bare day-dir CONTAINERS rather than real leaf
     * partitions) and C3/T-I3 ({@code rebuildSymbolFilesForColumns} never rebuilds the dedicated
     * dimension dictionaries or the {@code _cell} registry) are exactly why this is now refused rather
     * than silently attempted. {@link #testCheckpointRestoreAllowsNeverCommittedCompositeTable()} below
     * proves the companion half of the gate -- a composite table that has genuinely never routed a cell
     * is NOT refused. A plain table's checkpoint/restore is completely untouched by this gate (dimCount
     * == 0) and already exhaustively covered by {@code io.questdb.test.griffin.CheckpointTest}.
     * <p>
     * <b>Deliberately NOT wrapped in {@code assertMemoryLeak}:</b> investigated empirically (negative
     * control) rather than assumed, by the ORIGINAL version of this test. {@code
     * engine.checkpointRecover()} leaves a small, constant (1336-byte, i.e. one fixed-size allocation)
     * {@code NATIVE_TABLE_READER} tag difference behind when driven from an ordinary {@code
     * AbstractCairoTest} subclass -- reproduced with a 100% vanilla, non-composite, unindexed,
     * un-partitioned-or-partitioned table built via plain {@code CREATE TABLE}/{@code INSERT} AND via
     * {@code CREATE TABLE AS SELECT}, and even with a byte-for-byte copy of {@code
     * CheckpointTest#testRecoverCheckpointLargePartitionCount}'s own body pasted verbatim into a
     * throwaway class in this package. It reproduces regardless of composite-ness, partitioning, symbol
     * columns/indexes, CTAS vs. separate insert, or this class's O3PartitionPurgeJob field (each ruled
     * out individually) -- i.e. it is a pre-existing property of running checkpoint recovery outside
     * {@code io.questdb.test.griffin.CheckpointTest}'s own specialized fixture, not anything introduced
     * by Plan 3b/4a or specific to a composite table. {@code engine.clear()} at the top substitutes for
     * what {@code assertMemoryLeak} would have done on entry.
     */
    @Test
    public void testCheckpointRestoreRefusesRoutedCompositeTable() throws Exception {
        final String snapshotId = "00000000-0000-0000-0000-000000000000";
        final String restartedId = "123e4567-e89b-12d3-a456-426614174000";

        engine.clear();
        setProperty(PropertyKey.CAIRO_LEGACY_SNAPSHOT_INSTANCE_ID, snapshotId);

        // Only c (composite) in this checkpoint: DatabaseCheckpointAgent#recover aborts its WHOLE
        // per-table iteration on the first table that throws, so a twin p alongside c here would never
        // even get a chance to restore, muddying what this test is isolating.
        execute("create table c (ts timestamp, exchange symbol, px double) timestamp(ts) partition by day, exchange wal");
        execute("insert into c values ('2020-01-01T00:00:00.000000Z','A',1.0), ('2020-01-02T00:00:00.000000Z','A',2.0)");
        drainWalQueue();

        execute("checkpoint create");

        // Release all readers/writers but keep the checkpoint dir around (simulates a restart), then
        // force checkpointRecover() to actually attempt a restore (not no-op) by making the configured
        // snapshot instance id differ from the one the checkpoint was created under.
        engine.clear();
        setProperty(PropertyKey.CAIRO_LEGACY_SNAPSHOT_INSTANCE_ID, restartedId);
        try {
            engine.checkpointRecover();
            Assert.fail("expected checkpoint restore of a routed composite table to be refused");
        } catch (CairoException e) {
            TestUtils.assertContains(
                    e.getFlyweightMessage(),
                    "composite partitioning does not yet support checkpoint/snapshot restore");
        } finally {
            // Mirrors CheckpointTest#testCheckpointRestoreIndexNonPartitioned's own closing call (and its
            // class-wide @After safety net "checkpoint release"): checkpointRecover() does NOT itself
            // clear DatabaseCheckpointAgent's in-progress flag -- even when it throws -- only
            // checkpointRelease() does. Without this, "in progress" stays true for the rest of this
            // class's run (this class has no CheckpointTest-style @After net), and ColumnPurgeOperator
            // #purge0 unconditionally defers/refuses every purge while a checkpoint looks in progress.
            engine.checkpointRelease();
            engine.releaseInactive();
            engine.clear();
        }
    }

    /**
     * Companion to {@link #testCheckpointRestoreRefusesRoutedCompositeTable()}: a composite table that
     * has NEVER been committed -- the {@code _cell} registry is still empty, no cell has ever actually
     * been routed -- must NOT be refused by the C2/C3 guard. Mirrors {@code TableSnapshotRestore
     * #rebuildTableFiles}'s own documented condition (dimCount &gt; 0 AND the registry non-empty).
     */
    @Test
    public void testCheckpointRestoreAllowsNeverCommittedCompositeTable() throws Exception {
        final String snapshotId = "00000000-0000-0000-0000-000000000000";
        final String restartedId = "123e4567-e89b-12d3-a456-426614174000";

        engine.clear();
        setProperty(PropertyKey.CAIRO_LEGACY_SNAPSHOT_INSTANCE_ID, snapshotId);

        execute("create table c (ts timestamp, exchange symbol, px double) timestamp(ts) partition by day, exchange wal");

        execute("checkpoint create");

        engine.clear();
        setProperty(PropertyKey.CAIRO_LEGACY_SNAPSHOT_INSTANCE_ID, restartedId);
        engine.checkpointRecover(); // must NOT throw -- registry is empty, never committed

        assertQuery("select count() from c").noLeakCheck().noRandomAccess().expectSize().returns("count\n0\n");

        // c must still be fully writable/queryable post-restore.
        execute("insert into c values ('2020-01-01T00:00:00.000000Z','A',1.0)");
        drainWalQueue();
        assertQuery("select count() from c").noLeakCheck().noRandomAccess().expectSize().returns("count\n1\n");

        engine.checkpointRelease();
        engine.releaseInactive();
        engine.clear();
    }

    /**
     * Builds the composite table {@code c} ({@code partition by day, exchange}) and its plain twin
     * {@code p} ({@code partition by day}), then inserts byte-for-byte identical rows into both: 5 day
     * partitions (2020-01-01 .. 2020-01-05), 2 rows per day -- 10 rows total.
     * <p>
     * <b>Whole-branch review (Plan 4a) finding I1 update:</b> {@code c} was originally created WITHOUT
     * {@code WAL} and every row landed at cellKey 0 via the direct, non-WAL append path (real (ts,
     * cellKey) write-routing never engaged at all for it). I1 now rejects a non-WAL composite table at
     * CREATE (see {@code CreateTableOperationBuilderImpl#resolvePartitionSpec}), so {@code c} is now
     * created {@code WAL} -- which means it DOES route for real from its first commit (Plan 4a Task 4).
     * Every row still uses the SAME single {@code exchange} value ({@code 'A'}) -- deliberately: this
     * makes every day resolve to the SAME (only-ever-interned) cellKey 0, so {@code c}'s on-disk shape
     * stays exactly one physical partition per day (byte-identical topology to the original dormant
     * assumption this class's hardcoded partition/row counts were built against), while still being
     * genuinely, actually routed (not bypassing the routing machinery the way the old non-WAL path did).
     * This keeps this class's own scope narrow -- proving composite-vs-plain equivalence across
     * maintenance paths -- without also taking on Plan 4b's separate, not-yet-audited question of
     * whether those SAME paths are cell-AWARE for a day with 2+ real cells.
     */
    private void createAndPopulateTwins() throws Exception {
        execute("create table c (ts timestamp, exchange symbol, px double) timestamp(ts) partition by day, exchange wal");
        execute("create table p (ts timestamp, exchange symbol, px double) timestamp(ts) partition by day");

        final String rows = " values " +
                "('2020-01-01T00:00:00.000000Z','A',1.0), ('2020-01-01T12:00:00.000000Z','A',1.5), " +
                "('2020-01-02T00:00:00.000000Z','A',2.0), ('2020-01-02T12:00:00.000000Z','A',2.5), " +
                "('2020-01-03T00:00:00.000000Z','A',3.0), ('2020-01-03T12:00:00.000000Z','A',3.5), " +
                "('2020-01-04T00:00:00.000000Z','A',4.0), ('2020-01-04T12:00:00.000000Z','A',4.5), " +
                "('2020-01-05T00:00:00.000000Z','A',5.0), ('2020-01-05T12:00:00.000000Z','A',5.5)";
        execute("insert into c" + rows);
        execute("insert into p" + rows);
        drainWalQueue();
    }
}
