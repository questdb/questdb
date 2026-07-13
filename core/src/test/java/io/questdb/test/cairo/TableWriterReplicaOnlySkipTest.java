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

package io.questdb.test.cairo;

import io.questdb.PropertyKey;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Verifies the write-path behaviour of Task 11: on a "skipping primary" node (one where
 * {@link io.questdb.cairo.CairoConfiguration#skipReplicaOnlyIndexes()} returns true), a SYMBOL column
 * whose index is flagged REPLICA ONLY has no bitmap/posting index built or maintained -- no
 * {@code k./v.} (or posting {@code .pk/.pv}) files are created and no per-row index work runs -- while
 * the metadata still records the {@code indexed} + {@code replicaOnly} flags so a replica or a promoted
 * node can build the index later.
 * <p>
 * Note: full-scan query correctness on a skipping primary is gated on the planner index-eligibility
 * guard (Task 12, {@link io.questdb.cairo.sql.RecordMetadata#isColumnIndexActive}). Until that lands,
 * the planner still keys off {@code isColumnIndexed} and emits an index scan over the (absent) index,
 * so this test asserts the write-path invariants only.
 */
public class TableWriterReplicaOnlySkipTest extends AbstractCairoTest {

    // Default true so the legacy skipping-primary tests are unaffected; the convert-partition
    // regression below flips this at runtime to drive a role change (skip true -> false).
    private static volatile boolean skip = true;

    @BeforeClass
    public static void setUpStatic() throws Exception {
        skip = true;
        configurationFactory = (root, telemetry, overrides) ->
                new CairoTestConfiguration(root, telemetry, overrides) {
                    @Override
                    public boolean skipReplicaOnlyIndexes() {
                        return skip;
                    }
                };
        AbstractCairoTest.setUpStatic();
    }

    @Before
    public void resetSkip() {
        // Every legacy test in this class expects a skipping primary; restore the default before each
        // test so the convert-partition regression (which flips skip to false) cannot leak state.
        skip = true;
    }

    @Test
    public void testAlterAddIndexReplicaOnlySkipsBuild() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (s symbol capacity 256, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into x values ('a', 0), ('b', 1000000), ('a', 2000000)");
            drainWalQueue();

            execute("alter table x alter column s add index replica only");
            drainWalQueue();

            Assert.assertFalse(
                    "no index files expected on skipping primary after ALTER ADD INDEX REPLICA ONLY",
                    ReplicaOnlyIndexTestUtils.indexFilesExist(engine, "x", "s")
            );
            assertMetadataFlags("x", "s");
        });
    }

    // Regression: replica-only POSTING index + reconcile-rebuild + CONVERT PARTITION TO PARQUET must
    // not suspend the table, even when the index is UNMATERIALIZED at convert time.
    //
    // Reproduces the fuzz seed 0xABCD convert-suspend: a replica-only POSTING index is added while
    // skipping (so nothing is materialized), the role flips to NON-skipping (bumpRoleGeneration) with
    // NO intervening insert -- so reconcile (which only runs on a WAL insert apply) has NOT rebuilt the
    // index -- and then CONVERT PARTITION TO PARQUET runs. The convert/link path used to hard-link the
    // absent .pk/.pv.<sealTxn> and throw "index files do not exist", suspending the table. The fix in
    // TableWriter.copyOrRebuildColumnIndexes / linkPartitionIndexFiles tolerates the absence and skips
    // the column; a later insert-apply reconcile materializes the index over the now-parquet partition.
    @Test
    public void testReplicaOnlyPostingConvertToParquetUnmaterializedNoSuspend() throws Exception {
        assertMemoryLeak(() -> {
            skip = true; // ADD INDEX REPLICA ONLY does not materialize while skipping
            execute("create table x (" +
                    "c symbol capacity 256 index type posting replica only, " +
                    "v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into x values ('a', 1.0, 0), ('b', 2.0, 1000000), ('a', 3.0, 2000000)");
            drainWalQueue();
            Assert.assertFalse("posting index must NOT exist while skipping", ReplicaOnlyIndexTestUtils.indexFilesExist(engine, "x", "c"));

            // Flip to NON-skipping with a role bump but NO intervening insert: reconcile has not run, so
            // the replica-only posting index is still unmaterialized on disk.
            skip = false;
            engine.bumpRoleGeneration();

            // Convert to parquet must NOT suspend despite the absent .pk/.pv.
            execute("alter table x convert partition to parquet where ts >= 0");
            drainWalQueue();

            final TableToken token = engine.verifyTableName("x");
            Assert.assertFalse(
                    "table must not be suspended after CONVERT PARTITION on unmaterialized replica-only posting index",
                    engine.getTableSequencerAPI().isSuspended(token)
            );

            // count(*) stays correct.
            assertQuery("select count() from x").expectSize().noRandomAccess().returns("count\n3\n");

            // An insert now triggers reconcile, which materializes the posting index over the parquet
            // partition (indexHistoricPartitions/indexParquetPartition) -- correctness on a non-skipping
            // node: covered-value counts match a full-scan reference.
            execute("insert into x values ('a', 4.0, 3000000)");
            drainWalQueue();
            Assert.assertFalse(
                    "table must not be suspended after reconcile build over the parquet partition",
                    engine.getTableSequencerAPI().isSuspended(token)
            );

            // c='a' has 3 rows (rows 0,2 from native-then-parquet partition + the new native row).
            assertQuery("select count() from x where c = 'a'").expectSize().noRandomAccess().returns("count\n3\n");
            // Full distribution as an independent oracle.
            assertQuery("select c, count() from x order by c").expectSize().returns("c\tcount\na\t3\nb\t1\n");
        });
    }


    // Regression for an O3 crash: out-of-order rows spanning multiple partitions drive the O3 open-
    // column path (O3PartitionJob.publishOpenColumnTasks), which counts indexed columns to allocate
    // O3Basket indexer slots. The basket is sized from the writer's denseIndexers/indexCount, which
    // EXCLUDE a skipped REPLICA ONLY index; if the open-column loop still treats the column as indexed
    // (raw isColumnIndexed) it calls O3Basket.nextIndexer() one time too many and overruns the slot
    // list (an AssertionError in O3Basket.nextIndexer). The fix gates both on isColumnIndexActive().
    @Test
    public void testPrimarySkipsReplicaOnlyIndexBuildO3MultiPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (s symbol index capacity 256 replica only, v double, ts timestamp) timestamp(ts) partition by day wal");
            // Out-of-order rows across two partitions (1970-01-01 and 1970-01-02): forces the O3 path.
            execute("insert into x values" +
                    " ('a', 1, 0)," +
                    " ('c', 4, 86400000000)," +     // 1970-01-02
                    " ('b', 2, 1000000)," +
                    " ('a', 5, 86401000000)," +     // 1970-01-02
                    " ('a', 3, 2000000)");
            drainWalQueue();

            Assert.assertFalse("no index files expected on skipping primary after O3 multi-partition insert",
                    ReplicaOnlyIndexTestUtils.indexFilesExist(engine, "x", "s"));
            assertMetadataFlags("x", "s");
            // Full-scan correctness: the skipped index must not change query results.
            assertQuery("select s, v, ts from x where s = 'a'").timestamp("ts").returns("s\tv\tts\n" +
                            "a\t1.0\t1970-01-01T00:00:00.000000Z\n" +
                            "a\t3.0\t1970-01-01T00:00:02.000000Z\n" +
                            "a\t5.0\t1970-01-02T00:00:01.000000Z\n");
        });
    }

    // parquet -> native conversion rebuilds the per-partition bitmap index for indexed symbol
    // columns (TableWriter.rebuildPartitionIndexFiles). On a skipping primary a REPLICA ONLY
    // index must NOT be rebuilt during the conversion back to native, otherwise the offload
    // invariant is violated for any partition that was round-tripped through parquet.
    @Test
    public void testPrimarySkipsReplicaOnlyIndexBuildOnParquetToNativeConversion() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (s symbol index capacity 256 replica only, ts timestamp) timestamp(ts) partition by day wal");
            // Spread several rows across one day partition.
            execute("insert into x values ('a', 0), ('b', 1000000), ('a', 2000000), ('c', 3000000)");
            drainWalQueue();

            // No index built on the skipping primary at insert time.
            Assert.assertFalse("no index files expected on skipping primary after insert", ReplicaOnlyIndexTestUtils.indexFilesExist(engine, "x", "s"));

            // Round-trip the partition through parquet and back to native.
            execute("alter table x convert partition to parquet where ts >= 0");
            drainWalQueue();
            execute("alter table x convert partition to native where ts >= 0");
            drainWalQueue();

            // The conversion back to native must not have rebuilt the replica-only index.
            Assert.assertFalse(
                    "no index files expected on skipping primary after parquet->native conversion",
                    ReplicaOnlyIndexTestUtils.indexFilesExist(engine, "x", "s")
            );
            assertMetadataFlags("x", "s");

            // Full-scan correctness: the skipped index must not change query results.
            assertQuery("select s, ts from x where s = 'a'").timestamp("ts").returns("s\tts\n" +
                            "a\t1970-01-01T00:00:00.000000Z\n" +
                            "a\t1970-01-01T00:00:02.000000Z\n");
        });
    }

    // native -> parquet conversion (TableWriter.copyOrRebuildColumnIndexes, reached from
    // convertPartitionNativeToParquet) copies/rebuilds the per-partition bitmap index for indexed
    // symbol columns: it hard-links the existing .k/.v trio when columnTop==0, and rebuilds the
    // index (synthesizing the NULL prefix) when columnTop>0. On a skipping primary a REPLICA ONLY
    // index was never built, so BOTH branches must be skipped -- otherwise the colTop==0 link branch
    // throws "index files do not exist" and the colTop>0 branch wrongly materializes the index.
    // This test exercises both branches in one partition: column s exists for the whole partition
    // (colTop==0) while column s2 is added via ALTER after rows already exist (colTop>0).
    @Test
    public void testPrimarySkipsReplicaOnlyIndexBuildOnNativeToParquetConversion() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (s symbol index capacity 256 replica only, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into x values ('a', 0), ('b', 1000000), ('a', 2000000), ('c', 3000000)");
            drainWalQueue();

            // Add a SECOND symbol column AFTER rows exist, then flag it replica-only-indexed, so its
            // column top in the already-populated partition is > 0 (exercises the rebuild branch of
            // the loop, while s exercises the hard-link branch with columnTop == 0).
            execute("alter table x add column s2 symbol capacity 256");
            drainWalQueue();
            execute("alter table x alter column s2 add index replica only");
            drainWalQueue();
            execute("insert into x (s, ts, s2) values ('a', 4000000, 'x'), ('b', 5000000, 'y')");
            drainWalQueue();

            final TableToken token = engine.verifyTableName("x");

            // No index files for either replica-only column on the skipping primary before conversion.
            Assert.assertFalse("no s index files expected before conversion", ReplicaOnlyIndexTestUtils.indexFilesExist(engine, "x", "s"));
            Assert.assertFalse("no s2 index files expected before conversion", ReplicaOnlyIndexTestUtils.indexFilesExist(engine, "x", "s2"));

            // Convert the populated partition native -> parquet. Without the guard in
            // copyOrRebuildColumnIndexes the link branch (colTop==0, column s) throws
            // "index files do not exist", suspending the WAL table.
            execute("alter table x convert partition to parquet where ts >= 0");
            drainWalQueue();

            // The conversion must have succeeded (table not suspended) and built no index files.
            Assert.assertFalse(
                    "WAL table must not be suspended by native->parquet conversion of replica-only index",
                    engine.getTableSequencerAPI().isSuspended(token)
            );
            Assert.assertFalse(
                    "no s index files expected after native->parquet conversion",
                    ReplicaOnlyIndexTestUtils.indexFilesExist(engine, "x", "s")
            );
            Assert.assertFalse(
                    "no s2 index files expected after native->parquet conversion",
                    ReplicaOnlyIndexTestUtils.indexFilesExist(engine, "x", "s2")
            );
            assertMetadataFlags("x", "s");
            assertMetadataFlags("x", "s2");

            // Full-scan correctness over the parquet partition: results must be unaffected.
            assertQuery("select s, s2, ts from x where s = 'a'").timestamp("ts").returns("s\ts2\tts\n" +
                            "a\t\t1970-01-01T00:00:00.000000Z\n" +
                            "a\t\t1970-01-01T00:00:02.000000Z\n" +
                            "a\tx\t1970-01-01T00:00:04.000000Z\n");
        });
    }

    @Test
    public void testPrimarySkipsReplicaOnlyIndexBuild() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (s symbol index capacity 256 replica only, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into x values ('a', 0), ('b', 1000000), ('a', 2000000)");
            drainWalQueue();

            Assert.assertFalse("no index files expected on skipping primary", ReplicaOnlyIndexTestUtils.indexFilesExist(engine, "x", "s"));
            assertMetadataFlags("x", "s");
            // symbol dictionary must still be built (only the bitmap index is skipped):
            // the per-column symbol map files (s.o/s.c/s.k/s.v at the table root) are always present.
            assertSymbolDictExists("x", "s");
        });
    }

    // REINDEX (IndexBuilder.isSupportedColumn) rebuilds the bitmap index for every indexed symbol
    // column. On a skipping primary a REPLICA ONLY index must be excluded, otherwise an operator
    // running REINDEX would materialise the very index the node is meant to offload to replicas.
    // With the gate, a table whose only indexed column is replica-only has no reindexable column,
    // so a whole-table REINDEX reports "Table does not have any indexes" and builds nothing -- the
    // same outcome as a table with no indexes at all. The observable invariant is: no index files.
    @Test
    public void testReindexSkipsReplicaOnlyIndexOnSkippingPrimary() throws Exception {
        assertMemoryLeak(() -> {
            // BYPASS WAL: REINDEX TABLE ... LOCK EXCLUSIVE operates on the table directly.
            execute("create table x (s symbol index capacity 256 replica only, ts timestamp) timestamp(ts) partition by day bypass wal");
            execute("insert into x values ('a', 0), ('b', 1000000), ('a', 2000000)");
            engine.releaseAllWriters();

            // Nothing built at insert time on the skipping primary.
            Assert.assertFalse("no index files expected on skipping primary before REINDEX", ReplicaOnlyIndexTestUtils.indexFilesExist(engine, "x", "s"));

            // The replica-only column is excluded by the IndexBuilder gate, so the whole-table
            // REINDEX finds no reindexable column and reports it as un-indexed (instead of building).
            try {
                execute("REINDEX TABLE x LOCK EXCLUSIVE");
                Assert.fail("REINDEX should find no reindexable column on a skipping primary");
            } catch (io.questdb.cairo.CairoException e) {
                io.questdb.test.tools.TestUtils.assertContains(e.getFlyweightMessage(), "Table does not have any indexes");
            }
            engine.releaseAllWriters();
            engine.releaseAllReaders();

            // REINDEX must not have built the replica-only index on the skipping primary.
            Assert.assertFalse("no index files expected on skipping primary after REINDEX", ReplicaOnlyIndexTestUtils.indexFilesExist(engine, "x", "s"));
            assertMetadataFlags("x", "s");
        });
    }

    // ALTER COLUMN ... SYMBOL CAPACITY rebuilds the column metadata and rebinds the indexer. On a
    // skipping primary the replica-only column has NO wired indexer, so the indexer-rebind block in
    // TableWriter.changeSymbolCapacity would assert/NPE and distress the writer. The capacity change
    // also rebuilds TableColumnMetadata via updateColumnSymbolCapacity, which must carry the
    // replicaOnly flag over, otherwise rewriteAndSwapMetadata persists replicaOnly=false.
    @Test
    public void testChangeSymbolCapacityPreservesReplicaOnlyFlagAndDoesNotDistress() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (s symbol index capacity 256 replica only, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into x values ('a', 0), ('b', 1000000), ('a', 2000000)");
            drainWalQueue();

            final TableToken token = engine.verifyTableName("x");

            execute("alter table x alter column s symbol capacity 512");
            drainWalQueue();

            // (1) the writer must not be distressed / the table must not be suspended
            Assert.assertFalse(
                    "WAL table must not be suspended by ALTER COLUMN SYMBOL CAPACITY on a replica-only index",
                    engine.getTableSequencerAPI().isSuspended(token)
            );

            // (2) the indexed + replicaOnly flags must SURVIVE the capacity change
            assertMetadataFlags("x", "s");

            // (3) no index files materialised on the skipping primary
            Assert.assertFalse(
                    "no index files expected on skipping primary after SYMBOL CAPACITY change",
                    ReplicaOnlyIndexTestUtils.indexFilesExist(engine, "x", "s")
            );

            // table remains queryable; the skipped index must not change results
            assertQuery("select s, ts from x where s = 'a'").timestamp("ts").returns("s\tts\n" +
                            "a\t1970-01-01T00:00:00.000000Z\n" +
                            "a\t1970-01-01T00:00:02.000000Z\n");
        });
    }

    // TRUNCATE on a NON-partitioned table reaches TableWriter.truncateColumns, which resets the
    // indexer column top for indexed columns. On a skipping primary the replica-only column has no
    // wired indexer, so a raw indexers.get(i).resetColumnTop() would NPE. (Partitioned TRUNCATE goes
    // through releaseIndexerWriters, which is already null-safe.) Non-partitioned tables cannot use
    // WAL, so this exercises the direct (BYPASS WAL) writer path where truncateColumns runs.
    @Test
    public void testTruncateNonPartitionedReplicaOnlyIndexDoesNotCrash() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (s symbol index capacity 256 replica only, ts timestamp) timestamp(ts) partition by none bypass wal");
            execute("insert into x values ('a', 0), ('b', 1000000), ('a', 2000000)");
            engine.releaseAllWriters();

            // TRUNCATE on the non-partitioned table must not NPE on the absent indexer.
            execute("truncate table x");
            engine.releaseAllWriters();
            engine.releaseAllReaders();

            assertMetadataFlags("x", "s");

            // empty + queryable
            assertQuery("select count() from x").expectSize().noRandomAccess().returns("count\n0\n");
        });
    }

    // DROP INDEX on a replica-only-indexed column reaches TableWriter.removeIndex, which unwires the
    // indexer via indexers.getQuick(columnIndex). On a skipping primary the replica-only column has NO
    // wired indexer, so indexers can be shorter than columnCount: a raw getQuick(columnIndex) is an
    // out-of-bounds access (AssertionError under -ea / stale read in prod). The fix uses the bounds-
    // and null-safe getQuiet. BYPASS WAL: DROP INDEX runs on the writer directly.
    @Test
    public void testDropIndexOnReplicaOnlyIndexDoesNotCrash() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (s symbol index capacity 256 replica only, ts timestamp) timestamp(ts) partition by day bypass wal");
            execute("insert into x values ('a', 0), ('b', 1000000), ('a', 2000000)");
            engine.releaseAllWriters();

            // Nothing built on the skipping primary at insert time.
            Assert.assertFalse("no index files expected on skipping primary before DROP INDEX", ReplicaOnlyIndexTestUtils.indexFilesExist(engine, "x", "s"));

            // DROP INDEX must not OOB on the absent indexer slot.
            execute("alter table x alter column s drop index");
            engine.releaseAllWriters();
            engine.releaseAllReaders();

            // The index is gone: column flagged un-indexed AND the replica-only flag cleared.
            final TableToken token = engine.verifyTableName("x");
            try (TableReader reader = engine.getReader(token)) {
                final int colIdx = reader.getMetadata().getColumnIndex("s");
                Assert.assertFalse("column should no longer be indexed after DROP INDEX",
                        reader.getMetadata().isColumnIndexed(colIdx));
                Assert.assertFalse("replica-only flag should be cleared after DROP INDEX",
                        reader.getMetadata().isColumnReplicaOnlyIndex(colIdx));
            }
            Assert.assertFalse("no index files expected after DROP INDEX", ReplicaOnlyIndexTestUtils.indexFilesExist(engine, "x", "s"));

            // table remains queryable
            assertQuery("select s, ts from x where s = 'a'").timestamp("ts").returns("s\tts\n" +
                            "a\t1970-01-01T00:00:00.000000Z\n" +
                            "a\t1970-01-01T00:00:02.000000Z\n");
        });
    }

    // O3 commit on a table with a replica-only POSTING index drives the posting-seal housekeeping
    // sweep (TableWriter.sealPostingIndexesForO3Partitions -> sealPostingIndexForPartition ->
    // restorePostingIndexersToLastPartition). hasPostingIndex() keys off raw isColumnIndexed, so the
    // sweep DOES run on a skipping primary, and its per-column loop calls indexers.getQuick(colIdx)
    // over columnCount. The skipped replica-only column has no wired indexer, so that get is an
    // out-of-bounds access (AssertionError under -ea). The fix bounds-guards each posting loop with
    // colIdx >= indexers.size(), mirroring the fast-lag posting path.
    @Test
    public void testO3PostingSealSweepReplicaOnlyDoesNotCrash() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (s symbol index type posting replica only, v double, ts timestamp) timestamp(ts) partition by day wal");
            // In-order seed in two partitions.
            execute("insert into x values ('a', 1, 0), ('b', 2, 1000000), ('c', 3, 86400000000)");
            drainWalQueue();

            // Out-of-order insert into an already-committed partition: forces the O3 path and the
            // subsequent posting-seal sweep over the (un-wired) replica-only posting column.
            execute("insert into x values ('a', 4, 500000), ('b', 5, 1500000), ('a', 6, 86400500000)");
            drainWalQueue();

            final TableToken token = engine.verifyTableName("x");
            Assert.assertFalse(
                    "WAL table must not be suspended by O3 posting-seal sweep over a replica-only index",
                    engine.getTableSequencerAPI().isSuspended(token)
            );
            Assert.assertFalse("no posting index files expected on skipping primary after O3", ReplicaOnlyIndexTestUtils.indexFilesExist(engine, "x", "s"));
            assertMetadataFlags("x", "s");

            // Full-scan correctness across both partitions.
            assertQuery("select s, v, ts from x where s = 'a' order by ts").timestamp("ts").returns("s\tv\tts\n" +
                            "a\t1.0\t1970-01-01T00:00:00.000000Z\n" +
                            "a\t4.0\t1970-01-01T00:00:00.500000Z\n" +
                            "a\t6.0\t1970-01-02T00:00:00.500000Z\n");
        });
    }

    // Covering POSTING reseal over a parquet partition (TableWriter.resealParquetCoveringForPartition,
    // reached when sealPostingIndexForPartition sees a parquet partition) also loops every indexed
    // posting column and calls indexers.getQuick(colIdx). A replica-only covering-posting column on a
    // skipping primary has no wired indexer -> out-of-bounds get without the bounds guard.
    @Test
    public void testParquetCoveringPostingResealReplicaOnlyDoesNotCrash() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (s symbol index type posting include (v) replica only, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into x values ('a', 1, 0), ('b', 2, 1000000), ('c', 3, 2000000)");
            drainWalQueue();

            // Round-trip the partition through parquet so a later O3 seal hits the parquet covering path.
            execute("alter table x convert partition to parquet where ts >= 0");
            drainWalQueue();

            // O3 insert into the (now parquet) partition: drives the covering-posting reseal sweep.
            execute("insert into x values ('a', 4, 500000), ('b', 5, 1500000)");
            drainWalQueue();

            final TableToken token = engine.verifyTableName("x");
            Assert.assertFalse(
                    "WAL table must not be suspended by parquet covering-posting reseal over a replica-only index",
                    engine.getTableSequencerAPI().isSuspended(token)
            );
            Assert.assertFalse("no posting index files expected on skipping primary", ReplicaOnlyIndexTestUtils.indexFilesExist(engine, "x", "s"));
            assertMetadataFlags("x", "s");

            assertQuery("select s, v, ts from x where s = 'a' order by ts").timestamp("ts").returns("s\tv\tts\n" +
                            "a\t1.0\t1970-01-01T00:00:00.000000Z\n" +
                            "a\t4.0\t1970-01-01T00:00:00.500000Z\n");
        });
    }

    // Partition split (splitLastPartition) and squash (squashPartitions) both copy columns through
    // FrameImpl.getContiguousFileFrameColumn, which used to read the raw isColumnIndexed flag. On a
    // skipping primary a REPLICA ONLY symbol index has no physical .k/.v, so the frame path must treat
    // the column as un-indexed (isColumnIndexActive): otherwise the split builds a .k/.v the node must
    // not materialize, and the squash opens a BitmapIndexWriter against the absent key file and throws
    // "index does not exist", distressing the writer. This test forces several splits with an O3 insert
    // and then squashes them; the fix keeps the writer healthy and materializes no index files.
    @Test
    public void testSplitAndSquashReplicaOnlyIndexDoesNotMaterializeOrDistress() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);
        node1.setProperty(PropertyKey.CAIRO_O3_LAST_PARTITION_MAX_SPLITS, 20);
        node1.setProperty(PropertyKey.CAIRO_O3_MID_PARTITION_MAX_SPLITS, 20);

        assertMemoryLeak(() -> {
            execute("create table x (s symbol index capacity 256 replica only, v double, ts timestamp) timestamp(ts) partition by day bypass wal");
            // Seed a single day-0 partition with an in-order run of 300 rows (all symbol 'a').
            execute("insert into x select 'a', x * 1.0, timestamp_sequence(0, 60000000L) from long_sequence(300)");
            engine.releaseAllWriters();

            Assert.assertFalse("no index files expected on skipping primary after seed insert", ReplicaOnlyIndexTestUtils.indexFilesExist(engine, "x", "s"));

            // Three O3 inserts landing near the tail of the seeded partition: with a split-min-size of 1
            // each one splits the last partition (rather than merging), so several split partitions
            // accumulate before the squash.
            execute("insert into x values ('b', -1, 290 * 60000000L)");
            execute("insert into x values ('b', -2, 280 * 60000000L)");
            execute("insert into x values ('b', -3, 270 * 60000000L)");
            engine.releaseAllWriters();

            final TableToken token = engine.verifyTableName("x");
            final long splitPartitionCount = selectLong("select count() from table_partitions('x')");
            Assert.assertTrue(
                    "test setup: O3 inserts must create split partitions before squashPartitions(), count=" + splitPartitionCount,
                    splitPartitionCount > 1
            );

            // The split path must not have materialized the replica-only index.
            Assert.assertFalse("no index files expected on skipping primary after O3 split", ReplicaOnlyIndexTestUtils.indexFilesExist(engine, "x", "s"));

            // Squash the split partitions back together. Without the FrameImpl fix this opens a
            // BitmapIndexWriter over the absent .k and throws, distressing the writer.
            try (TableWriter writer = TestUtils.getWriter(engine, token)) {
                writer.squashPartitions();
                Assert.assertFalse("writer must not be distressed by squashPartitions() over a replica-only index", writer.isDistressed());
            }
            engine.releaseAllWriters();
            engine.releaseAllReaders();

            final long squashedPartitionCount = selectLong("select count() from table_partitions('x')");
            Assert.assertTrue(
                    "squashPartitions() must reduce the split partition count [before=" + splitPartitionCount + ", after=" + squashedPartitionCount + ']',
                    squashedPartitionCount < splitPartitionCount
            );

            // Still no index files after the squash, and the metadata flags survive.
            Assert.assertFalse("no index files expected on skipping primary after squash", ReplicaOnlyIndexTestUtils.indexFilesExist(engine, "x", "s"));
            assertMetadataFlags("x", "s");

            // Full-scan correctness over the squashed partition: the skipped index must not change results.
            assertQuery("select s, count() from x order by s").noLeakCheck().expectSize().returns("s\tcount\na\t300\nb\t3\n");
        });
    }

    // changeColumnType removes the source column and re-adds the replacement through addColumnToMeta.
    // On a skipping primary a replica-only source column must not distress the writer or materialize an
    // index during the conversion, and the replica-only flag must be carried onto the replacement column
    // (addColumnToMeta now threads it through, matching the changeSymbolCapacity fix) so configureColumn's
    // skip gate fires. A same-type symbol change is rejected by SQL, so the still-indexed survival path is
    // not reachable here; this exercises the reachable conversion of a replica-only indexed symbol column.
    @Test
    public void testChangeColumnTypeReplicaOnlyIndexDoesNotMaterializeOrDistress() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (s symbol index capacity 256 replica only, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into x values ('a', 1, 0), ('b', 2, 1000000), ('a', 3, 2000000)");
            drainWalQueue();

            final TableToken token = engine.verifyTableName("x");
            Assert.assertFalse("no index files expected on skipping primary before type change", ReplicaOnlyIndexTestUtils.indexFilesExist(engine, "x", "s"));

            execute("alter table x alter column s type varchar");
            drainWalQueue();

            Assert.assertFalse(
                    "WAL table must not be suspended by ALTER COLUMN TYPE on a replica-only indexed column",
                    engine.getTableSequencerAPI().isSuspended(token)
            );
            Assert.assertFalse("no index files expected after type change", ReplicaOnlyIndexTestUtils.indexFilesExist(engine, "x", "s"));

            // The replacement column is a plain varchar: no longer indexed or replica-only.
            try (TableReader reader = engine.getReader(token)) {
                final int colIdx = reader.getMetadata().getColumnIndex("s");
                Assert.assertFalse("converted column must not be indexed", reader.getMetadata().isColumnIndexed(colIdx));
                Assert.assertFalse("converted column must not be replica-only", reader.getMetadata().isColumnReplicaOnlyIndex(colIdx));
            }

            assertQuery("select s, v, ts from x where s = 'a'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("s\tv\tts\na\t1.0\t1970-01-01T00:00:00.000000Z\na\t3.0\t1970-01-01T00:00:02.000000Z\n");
        });
    }

    // ATTACH PARTITION LIST of a partition whose _dmeta is absent (a backup / filesystem-copied
    // partition) makes attachPrepare return false, so ATTACH falls through to attachValidateMetadata ->
    // attachPartitionCheckSymbolColumn. That check read the raw indexed flag and demanded the index
    // .pk/.pv (or .k/.v) sidecars, which a replica-only index never materializes on a skipping primary
    // -> "Index key file does not exist" (on a WAL table this suspends it during the ATTACH apply). The
    // gate now skips the sidecar check for a replica-only column, mirroring copyOrRebuildColumnIndexes.
    @Test
    public void testAttachPartitionReplicaOnlyIndexMissingMetadataDoesNotRequireIndexFiles() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (s symbol index capacity 32 replica only, i int, ts timestamp) timestamp(ts) partition by day bypass wal");
            execute("insert into x values ('a', 1, 0), ('b', 2, 3600000000), ('a', 3, 86400000000)");
            engine.releaseAllWriters();
            Assert.assertFalse("no index files expected on skipping primary", ReplicaOnlyIndexTestUtils.indexFilesExist(engine, "x", "s"));

            execute("alter table x detach partition list '1970-01-01'");
            engine.releaseAllWriters();

            // Simulate a backup / filesystem-copied partition: drop _dmeta + _dcv from the detached dir
            // so attachPrepare returns false and ATTACH reaches attachValidateMetadata.
            final TableToken token = engine.verifyTableName("x");
            try (Path p = new Path()) {
                p.of(engine.getConfiguration().getDbRoot()).concat(token).concat("1970-01-01").put(TableUtils.DETACHED_DIR_MARKER).concat(TableUtils.META_FILE_NAME).$();
                Assert.assertTrue("remove _dmeta", TestUtils.remove(p.$()));
                p.parent().concat(TableUtils.COLUMN_VERSION_FILE_NAME).$();
                Assert.assertTrue("remove _dcv", TestUtils.remove(p.$()));
            }
            renameDetachedToAttachable("x", "1970-01-01");

            // Without the fix this throws "Index key file does not exist".
            execute("alter table x attach partition list '1970-01-01'");
            engine.releaseAllWriters();

            Assert.assertFalse("attach must not materialize the replica-only index", ReplicaOnlyIndexTestUtils.indexFilesExist(engine, "x", "s"));
            assertMetadataFlags("x", "s");
            assertQuery("select s, i, ts from x order by ts").timestamp("ts").expectSize().returns("s\ti\tts\n" +
                            "a\t1\t1970-01-01T00:00:00.000000Z\n" +
                            "b\t2\t1970-01-01T01:00:00.000000Z\n" +
                            "a\t3\t1970-01-02T00:00:00.000000Z\n");
        });
    }

    // ATTACH of a partition detached BEFORE the column gained its replica-only index drives the
    // attachPrepare rebuild branch (isIndexedNow && !wasIndexedAtDetached), which is now gated for a
    // replica-only column on a skipping primary (mirroring rebuildPartitionIndexFiles). NOTE: the gate
    // here is defensive/consistency-only -- rebuildAttachedPartitionColumnIndex runs reindexColumn with
    // an empty path root (attachIndexBuilder.of(Utf8String.EMPTY)) against the partition's FINAL native
    // path, which does not exist yet at attachPrepare time (the data is still in the ".attachable" dir,
    // renamed into place only later), so doReindex no-ops ("partition does not exist"). It therefore
    // cannot materialize a stale index today; the gate keeps the invariant robust if that path changes.
    // This test is end-to-end coverage of ATTACH over a replica-only-indexed column: it passes with or
    // without the gate (there is no materialization to reproduce), but pins that ATTACH stays clean.
    @Test
    public void testAttachPartitionDoesNotRebuildReplicaOnlyIndexOnSkippingPrimary() throws Exception {
        assertMemoryLeak(() -> {
            // s is a plain (un-indexed) symbol at detach time.
            execute("create table x (s symbol capacity 32, i int, ts timestamp) timestamp(ts) partition by day bypass wal");
            execute("insert into x values ('a', 1, 0), ('b', 2, 3600000000), ('a', 3, 86400000000)");
            engine.releaseAllWriters();

            execute("alter table x detach partition list '1970-01-01'");
            engine.releaseAllWriters();

            // Add the replica-only index after the detach: the skipping primary materializes nothing.
            execute("alter table x alter column s add index replica only");
            engine.releaseAllWriters();
            Assert.assertFalse("no index files expected on skipping primary after ADD INDEX REPLICA ONLY", ReplicaOnlyIndexTestUtils.indexFilesExist(engine, "x", "s"));

            // Keep _dmeta so attachPrepare runs and reaches the rebuild branch.
            renameDetachedToAttachable("x", "1970-01-01");
            execute("alter table x attach partition list '1970-01-01'");
            engine.releaseAllWriters();

            Assert.assertFalse("attach must not materialize the replica-only index on a skipping primary", ReplicaOnlyIndexTestUtils.indexFilesExist(engine, "x", "s"));
            assertMetadataFlags("x", "s");
            assertQuery("select s, i, ts from x order by ts").timestamp("ts").expectSize().returns("s\ti\tts\n" +
                            "a\t1\t1970-01-01T00:00:00.000000Z\n" +
                            "b\t2\t1970-01-01T01:00:00.000000Z\n" +
                            "a\t3\t1970-01-02T00:00:00.000000Z\n");
        });
    }

    @Test
    public void testPostingIndexOnReaddedParquetColumnSurvivesNativeConversionAndMetadataReload() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (a symbol, d symbol, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into x values ('x','old',0),('y','old',1000000)");
            drainWalQueue();
            execute("alter table x convert partition to parquet list '1970-01-01'");
            execute("alter table x drop column d");
            execute("alter table x add column d symbol");
            execute("alter table x alter column d add index type posting");
            drainWalQueue();
            execute("alter table x convert partition to native list '1970-01-01'");
            execute("insert into x (a,d,ts) values ('z','new',2000000)");
            drainWalQueue();
            execute("alter table x convert partition to parquet list '1970-01-01'");
            execute("truncate table x");
            execute("insert into x (a,d,ts) values ('z','new',2000000)");
            drainWalQueue();
            execute("alter table x drop column a");
            drainWalQueue();

            assertQuery("select count() from x where d = 'new'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\n1\n");
        });
    }

    @Test
    public void testTouchSkipsInactiveReplicaOnlyIndex() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (s SYMBOL INDEX REPLICA ONLY, v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO x VALUES ('a', 1, 0), ('b', 2, 1_000_000)");

            try (SqlCompiler compiler = engine.getSqlCompiler();
                 RecordCursorFactory factory = compiler.compile("SELECT touch((SELECT s, v, ts FROM x))", sqlExecutionContext).getRecordCursorFactory();
                 RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Assert.assertTrue(cursor.hasNext());
                TestUtils.assertContains(cursor.getRecord().getStrA(0), "data_pages");
                Assert.assertFalse(cursor.hasNext());
            }
            Assert.assertFalse(
                    "touch() must not require or materialize the inactive replica-only index",
                    ReplicaOnlyIndexTestUtils.indexFilesExist(engine, "x", "s")
            );
        });
    }

    @Test
    public void testUpdateUsesFullScanAndDoesNotMaterializeReplicaOnlyIndex() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (s SYMBOL INDEX REPLICA ONLY, v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO x VALUES ('a', 1, 0), ('b', 2, 1_000_000), ('a', 3, 2_000_000)");
            drainWalQueue();

            execute("UPDATE x SET v = v + 10 WHERE s = 'a'");
            drainWalQueue();

            Assert.assertFalse(
                    "UPDATE must not materialize the replica-only index on a skipping primary",
                    ReplicaOnlyIndexTestUtils.indexFilesExist(engine, "x", "s")
            );
            assertMetadataFlags("x", "s");
            assertQuery("SELECT s, v, ts FROM x ORDER BY ts")
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            s\tv\tts
                            a\t11.0\t1970-01-01T00:00:00.000000Z
                            b\t2.0\t1970-01-01T00:00:01.000000Z
                            a\t13.0\t1970-01-01T00:00:02.000000Z
                            """);
        });
    }

    // Rename "<partition>.detached" to the attach marker so ATTACH PARTITION LIST can pick it up.
    private void renameDetachedToAttachable(String table, String partition) {
        final TableToken token = engine.verifyTableName(table);
        try (Path from = new Path(); Path to = new Path()) {
            from.of(engine.getConfiguration().getDbRoot()).concat(token).concat(partition).put(TableUtils.DETACHED_DIR_MARKER).$();
            to.of(engine.getConfiguration().getDbRoot()).concat(token).concat(partition).put(engine.getConfiguration().getAttachPartitionSuffix()).$();
            Assert.assertTrue(Files.rename(from.$(), to.$()) > -1);
        }
    }

    // The metadata must still record indexed=true and replicaOnly=true so a replica or a promoted
    // node (skipReplicaOnlyIndexes()==false) can build the bitmap index later.
    private void assertMetadataFlags(String table, String col) {
        final TableToken token = engine.verifyTableName(table);
        try (TableReader reader = engine.getReader(token)) {
            final int colIdx = reader.getMetadata().getColumnIndex(col);
            Assert.assertTrue("column should be flagged indexed in metadata", reader.getMetadata().isColumnIndexed(colIdx));
            Assert.assertTrue("column should be flagged replicaOnly in metadata", reader.getMetadata().isColumnReplicaOnlyIndex(colIdx));
        }
    }

    // The symbol dictionary lives at the table root as "<col>.o" (offsets) and "<col>.c" (chars).
    // These must exist even when the bitmap index is skipped.
    private void assertSymbolDictExists(String table, String col) {
        final TableToken token = engine.verifyTableName(table);
        final FilesFacade ff = engine.getConfiguration().getFilesFacade();
        try (Path path = new Path()) {
            path.of(engine.getConfiguration().getDbRoot()).concat(token.getDirName()).concat(col).put(".o");
            Assert.assertTrue("symbol offset file (" + col + ".o) should exist", ff.exists(path.$()));
            path.of(engine.getConfiguration().getDbRoot()).concat(token.getDirName()).concat(col).put(".c");
            Assert.assertTrue("symbol char file (" + col + ".c) should exist", ff.exists(path.$()));
        }
    }

    private long selectLong(CharSequence sql) throws Exception {
        try (RecordCursorFactory factory = select(sql);
             RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            Assert.assertTrue("query must return one row [sql=" + sql + ']', cursor.hasNext());
            final long value = cursor.getRecord().getLong(0);
            Assert.assertFalse("query must return exactly one row [sql=" + sql + ']', cursor.hasNext());
            return value;
        }
    }
}
