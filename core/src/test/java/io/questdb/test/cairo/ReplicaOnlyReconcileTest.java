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

import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.cutlass.parquet.ParquetExportMode;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Verifies node-local reconcile of replica-only indexes on a role change, made
 * hot-switch-safe by the engine's role-generation counter.
 * <p>
 * The persisted schema flags ({@code indexed}, {@code replicaOnly}) are identical on every node;
 * whether the index is physically materialized is node-local and follows
 * {@link io.questdb.cairo.CairoConfiguration#skipReplicaOnlyIndexes()}. The role can flip at
 * runtime (hot promote/demote) WITHOUT reopening writers; the enterprise layer bumps
 * {@link io.questdb.cairo.CairoEngine#bumpRoleGeneration()} and an already-open {@code TableWriter}
 * self-heals on its next WAL apply ({@code reconcileReplicaOnlyIndexes}).
 * <p>
 * This test uses a MUTABLE skip flag so it can flip the simulated role at runtime.
 */
public class ReplicaOnlyReconcileTest extends AbstractCairoTest {

    private static final AtomicBoolean failPurge = new AtomicBoolean();
    private static final AtomicInteger skipCallCount = new AtomicInteger();
    private static volatile boolean skip;
    private static volatile int skipFromCall;
    private static FilesFacade testFf;

    @BeforeClass
    public static void setUpStatic() throws Exception {
        skip = false;
        testFf = new TestFilesFacadeImpl() {
            @Override
            public boolean removeQuiet(LPSZ name) {
                if (Utf8s.containsAscii(name, "1970-01-01")
                        && Utf8s.containsAscii(name, "/s.k")
                        && failPurge.compareAndSet(true, false)) {
                    return false;
                }
                return super.removeQuiet(name);
            }
        };
        ff = testFf;
        configurationFactory = (root, telemetry, overrides) ->
                new CairoTestConfiguration(root, telemetry, overrides) {
                    @Override
                    public FilesFacade getFilesFacade() {
                        return testFf;
                    }

                    @Override
                    public boolean skipReplicaOnlyIndexes() {
                        final int transitionCall = skipFromCall;
                        return transitionCall < Integer.MAX_VALUE
                                ? skipCallCount.incrementAndGet() >= transitionCall
                                : skip;
                    }
                };
        AbstractCairoTest.setUpStatic();
    }

    @Before
    public void resetRoleTransition() {
        failPurge.set(false);
        skipCallCount.set(0);
        skipFromCall = Integer.MAX_VALUE;
    }

    @Test
    public void testComputedProjectionParquetModePreservesRoleValidation() throws Exception {
        assertMemoryLeak(() -> {
            skip = false;
            execute("CREATE TABLE x (s SYMBOL INDEX REPLICA ONLY, v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO x VALUES ('a', 1, 0), ('b', 2, 1_000_000)");

            try (SqlCompiler compiler = engine.getSqlCompiler();
                 RecordCursorFactory factory = compiler.compile(
                         "SELECT s, v + 1 adjusted FROM x",
                         sqlExecutionContext
                 ).getRecordCursorFactory()) {
                Assert.assertEquals(
                        ParquetExportMode.PAGE_FRAME_BACKED,
                        ParquetExportMode.determineExportMode(factory, false, sqlExecutionContext)
                );

                skip = true;
                engine.bumpRoleGeneration();
                try (PageFrameCursor ignored = ParquetExportMode.getPageFrameBackedCursor(
                        factory,
                        sqlExecutionContext,
                        io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_ASC
                )) {
                    Assert.fail("page-frame-backed export must reject a stale role-sensitive factory");
                } catch (TableReferenceOutOfDateException expected) {
                    // expected
                }
            }

            execute("CREATE TABLE n (s SYMBOL, v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO n VALUES ('a', 1, 0), ('b', 2, 1_000_000)");
            try (SqlCompiler compiler = engine.getSqlCompiler();
                 RecordCursorFactory factory = compiler.compile(
                         "SELECT s, v + 1 adjusted FROM n",
                         sqlExecutionContext
                 ).getRecordCursorFactory()) {
                Assert.assertEquals(
                        ParquetExportMode.PAGE_FRAME_BACKED,
                        ParquetExportMode.determineExportMode(factory, false, sqlExecutionContext)
                );
                engine.bumpRoleGeneration();
                try (PageFrameCursor pageFrameCursor = ParquetExportMode.getPageFrameBackedCursor(
                        factory,
                        sqlExecutionContext,
                        io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_ASC
                )) {
                    Assert.assertNotNull("non-sensitive computed projection must remain executable", pageFrameCursor.next());
                }
            }
        });
    }

    @Test
    public void testReconcileBuildsOnOpenWhenReplica() throws Exception {
        assertMemoryLeak(() -> {
            // Replica/standalone node (skip=false): the replica-only index is materialized on apply.
            skip = false;
            execute("create table x (s symbol index capacity 256 replica only, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into x values ('a',1,0),('b',2,1_000_000),('a',3,2_000_000)");
            drainWalQueue();
            Assert.assertTrue("replica-only index files must exist after WAL apply on a replica", ReplicaOnlyIndexTestUtils.indexFilesExist(engine, "x", "s"));

            // Simulate restore-from-primary-backup: the backup lacks the index sidecars. Delete them
            // and drop all writers so the next access reopens the writer and reconciles on open.
            engine.releaseAllWriters();
            ReplicaOnlyIndexTestUtils.deleteIndexFiles(engine, "x", "s");
            Assert.assertFalse("index files must be gone after simulated restore", ReplicaOnlyIndexTestUtils.indexFilesExist(engine, "x", "s"));

            // Force a writer reopen + apply. The constructor reconcile rebuilds the missing index;
            // the apply then maintains it for the new row.
            execute("insert into x values ('b',4,3_000_000)");
            drainWalQueue();

            Assert.assertTrue("reconcile-on-open must rebuild the replica-only index files", ReplicaOnlyIndexTestUtils.indexFilesExist(engine, "x", "s"));
            assertIndexUsed();
            assertContents("s\tv\tts\n" +
                    "a\t1.0\t1970-01-01T00:00:00.000000Z\n" +
                    "a\t3.0\t1970-01-01T00:00:02.000000Z\n");
        });
    }

    @Test
    public void testReconcilePurgesFilesCreatedDuringOpenRoleTransition() throws Exception {
        assertMemoryLeak(() -> {
            skip = false;
            execute("CREATE TABLE x (s SYMBOL INDEX REPLICA ONLY, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO x VALUES ('a', 0)");
            engine.releaseAllWriters();
            ReplicaOnlyIndexTestUtils.deleteIndexFiles(engine, "x", "s");
            Assert.assertFalse(ReplicaOnlyIndexTestUtils.indexFilesExist(engine, "x", "s"));

            // The constructor samples the replica role while taking the absence snapshot, append
            // setup then fabricates empty index files, and reconciliation samples the primary role.
            // The call boundary is deterministic for this one-column schema.
            skipCallCount.set(0);
            skipFromCall = 4;
            final TableToken token = engine.verifyTableName("x");
            try (io.questdb.cairo.TableWriter ignored = engine.getWriter(token, "role transition")) {
                Assert.assertTrue("transition must occur during writer open", skipCallCount.get() >= skipFromCall);
            }

            Assert.assertFalse(
                    "open-time promotion must purge files fabricated after the absence snapshot",
                    ReplicaOnlyIndexTestUtils.indexFilesExist(engine, "x", "s")
            );
        });
    }

    @Test
    public void testReconcileRepairsMissingOlderPartitionIndex() throws Exception {
        assertMemoryLeak(() -> {
            skip = false;
            execute("create table x (s symbol index replica only, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into x values ('a',1,0),('b',2,86_400_000_000)");
            drainWalQueue();
            engine.releaseAllWriters();

            ReplicaOnlyIndexTestUtils.deleteIndexFilesInPartition(engine, "x", "s", "1970-01-01");
            execute("insert into x values ('a',3,86_401_000_000)");
            drainWalQueue();

            assertIndexUsed();
            assertContents("s\tv\tts\n" +
                    "a\t1.0\t1970-01-01T00:00:00.000000Z\n" +
                    "a\t3.0\t1970-01-02T00:00:01.000000Z\n");
        });
    }

    @Test
    public void testReconcileBuildsThenPurgesOnRoleFlip() throws Exception {
        assertMemoryLeak(() -> {
            // 1. Replica (skip=false): index materialized + used.
            skip = false;
            execute("create table x (s symbol index capacity 256 replica only, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into x values ('a',1,0),('b',2,1_000_000),('a',3,2_000_000)");
            drainWalQueue();
            Assert.assertTrue("index files must exist on a replica", ReplicaOnlyIndexTestUtils.indexFilesExist(engine, "x", "s"));
            assertIndexUsed();
            assertContents("s\tv\tts\n" +
                    "a\t1.0\t1970-01-01T00:00:00.000000Z\n" +
                    "a\t3.0\t1970-01-01T00:00:02.000000Z\n");

            // 2. Hot promotion to a skipping primary: flip the role, bump the generation, then an
            //    insert triggers the WAL-apply self-heal which PURGES the index sidecars.
            skip = true;
            engine.bumpRoleGeneration();
            execute("insert into x values ('a',4,4_000_000)");
            drainWalQueue();

            Assert.assertFalse("role flip to primary must purge the replica-only index files", ReplicaOnlyIndexTestUtils.indexFilesExist(engine, "x", "s"));
            assertIndexNotUsed();
            // metadata flags are KEPT (node-local materialization only):
            assertMetadataFlagsKept();
            assertContents("s\tv\tts\n" +
                    "a\t1.0\t1970-01-01T00:00:00.000000Z\n" +
                    "a\t3.0\t1970-01-01T00:00:02.000000Z\n" +
                    "a\t4.0\t1970-01-01T00:00:04.000000Z\n");

            // 3. Hot demotion back to a replica: flip role back, bump generation, apply -> REBUILD.
            skip = false;
            engine.bumpRoleGeneration();
            execute("insert into x values ('b',5,5_000_000)");
            drainWalQueue();

            Assert.assertTrue("role flip back to replica must rebuild the replica-only index files", ReplicaOnlyIndexTestUtils.indexFilesExist(engine, "x", "s"));
            assertIndexUsed();
            assertContents("s\tv\tts\n" +
                    "a\t1.0\t1970-01-01T00:00:00.000000Z\n" +
                    "a\t3.0\t1970-01-01T00:00:02.000000Z\n" +
                    "a\t4.0\t1970-01-01T00:00:04.000000Z\n");
        });
    }

    @Test
    public void testCachedIndexReaderSurvivesReloadAfterPromotePurge() throws Exception {
        // Reader-side companion to the purge. reloadColumnAt() re-opens a CACHED index reader via
        // IndexReader.of(...). closeIndexReader() frees a partition's cached reader but does NOT null
        // the slot, so after a partition's columns are closed the slot still holds a (closed) reader.
        // If a hot promote to a skipping primary purged the replica-only sidecars in the meantime, the
        // re-open hits a now-absent key file. For a replica-only column that must degrade gracefully
        // (drop the cached reader) rather than throw a CRITICAL corruption error that distresses the
        // reader and suspends the table.
        assertMemoryLeak(() -> {
            // 1. Replica (skip=false): two partitions, index materialized.
            skip = false;
            execute("create table x (s symbol index capacity 256 replica only, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into x values ('a',1,0),('b',2,1_000_000),('a',3,86_400_000_000)"); // day0 + day1
            drainWalQueue();
            Assert.assertTrue("index files must exist on a replica", ReplicaOnlyIndexTestUtils.indexFilesExist(engine, "x", "s"));

            final TableToken token = engine.verifyTableName("x");
            try (io.questdb.cairo.TableReader reader = engine.getReader(token)) {
                final int sIdx = reader.getMetadata().getColumnIndex("s");
                // Cache an index reader for partition 0 (day0): the slot becomes non-null.
                reader.openPartition(0);
                Assert.assertNotNull(reader.getIndexReader(0, sIdx, io.questdb.cairo.idx.IndexReader.DIR_BACKWARD));

                // 2. Hot promote to a skipping primary, then an O3 insert into partition 0. The WAL
                //    apply self-heals (purges the sidecars) and rewrites partition 0 (new nameTxn)
                //    WITHOUT rebuilding the index (skip=true).
                skip = true;
                engine.bumpRoleGeneration();
                execute("insert into x values ('a',4,500_000)"); // out-of-order within day0
                drainWalQueue();
                Assert.assertFalse("promote must purge the replica-only index files", ReplicaOnlyIndexTestUtils.indexFilesExist(engine, "x", "s"));

                // 3. Reload + re-open partition 0: closeRewrittenPartitionFiles closes its columns
                //    (slot freed but retained non-null) and reloadColumnAt re-opens the CACHED reader
                //    onto the now-absent index file. Must NOT throw a CRITICAL corruption error.
                reader.reload();
                reader.openPartition(0);
                // A later index-reader fetch must also stay graceful.
                reader.getIndexReaderIfExists(0, sIdx, io.questdb.cairo.idx.IndexReader.DIR_BACKWARD);
            }

            // 4. Table is healthy and not suspended: a full-scan query (planner skips the index on a
            //    primary) returns correct rows.
            assertIndexNotUsed();
            assertContents("s\tv\tts\n" +
                    "a\t1.0\t1970-01-01T00:00:00.000000Z\n" +
                    "a\t4.0\t1970-01-01T00:00:00.500000Z\n" +
                    "a\t3.0\t1970-01-02T00:00:00.000000Z\n");
        });
    }

    @Test
    public void testCachedNormalIndexReaderReloadStaysCritical() throws Exception {
        // Negative control for the reloadColumnAt() tolerance: the guard must apply ONLY to
        // replica-only columns. A NORMAL (non-replica-only) indexed column whose index file is
        // genuinely missing during a cached-reader reload must remain a CRITICAL corruption error.
        assertMemoryLeak(() -> {
            skip = false; // role is irrelevant for a normal index; it is never purged
            execute("create table n (s symbol index capacity 256, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into n values ('a',1,0),('b',2,1_000_000),('a',3,86_400_000_000)");
            drainWalQueue();
            Assert.assertTrue(ReplicaOnlyIndexTestUtils.indexFilesExist(engine, "n", "s"));

            final TableToken token = engine.verifyTableName("n");
            try (io.questdb.cairo.TableReader reader = engine.getReader(token)) {
                final int sIdx = reader.getMetadata().getColumnIndex("s");
                reader.openPartition(0);
                Assert.assertNotNull(reader.getIndexReader(0, sIdx, io.questdb.cairo.idx.IndexReader.DIR_BACKWARD));

                // Rewrite partition 0 (closes + reopens its columns), then delete the rebuilt index
                // files to simulate genuine corruption -- a normal index is never legitimately absent.
                execute("insert into n values ('a',4,500_000)");
                drainWalQueue();
                ReplicaOnlyIndexTestUtils.deleteIndexFiles(engine, "n", "s");

                boolean threw = false;
                try {
                    reader.reload();
                    reader.openPartition(0);
                } catch (io.questdb.cairo.CairoException e) {
                    threw = true;
                    Assert.assertTrue("a missing NORMAL index file must remain CRITICAL during reload", e.isCritical());
                }
                Assert.assertTrue("expected a critical error for the corrupted normal index", threw);
            }
        });
    }

    @Test
    public void testReconcileIfRoleChangedTriggerOnPooledWriter() throws Exception {
        assertMemoryLeak(() -> {
            // Replica (skip=false): index materialized on apply.
            skip = false;
            execute("create table x (s symbol index capacity 256 replica only, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into x values ('a',1,0),('b',2,1_000_000),('a',3,2_000_000)");
            drainWalQueue();
            Assert.assertTrue("index files must exist on a replica", ReplicaOnlyIndexTestUtils.indexFilesExist(engine, "x", "s"));

            final TableToken token = engine.verifyTableName("x");

            // Promote to a skipping primary, bump the generation, then drive the PUBLIC role-switch
            // sweep trigger directly on an already-open / pooled writer (no WAL apply): it must purge
            // the stale replica-only index sidecars.
            skip = true;
            engine.bumpRoleGeneration();
            try (io.questdb.cairo.TableWriter writer = engine.getWriter(token, "sweep")) {
                writer.reconcileReplicaOnlyIndexesIfRoleChanged();
            }
            Assert.assertFalse("role-switch sweep trigger must purge the replica-only index files", ReplicaOnlyIndexTestUtils.indexFilesExist(engine, "x", "s"));

            // Demote back to a replica, bump again, and trigger the sweep: it must rebuild the index.
            skip = false;
            engine.bumpRoleGeneration();
            try (io.questdb.cairo.TableWriter writer = engine.getWriter(token, "sweep")) {
                writer.reconcileReplicaOnlyIndexesIfRoleChanged();
                // Idempotent: a second call with no further gen bump is a cheap no-op.
                writer.reconcileReplicaOnlyIndexesIfRoleChanged();
            }
            Assert.assertTrue("role-switch sweep trigger must rebuild the replica-only index files", ReplicaOnlyIndexTestUtils.indexFilesExist(engine, "x", "s"));
            assertIndexUsed();
            assertContents("s\tv\tts\n" +
                    "a\t1.0\t1970-01-01T00:00:00.000000Z\n" +
                    "a\t3.0\t1970-01-01T00:00:02.000000Z\n");
        });
    }

    @Test
    public void testReconcileRetriesPartialPurge() throws Exception {
        assertMemoryLeak(() -> {
            skip = false;
            execute("CREATE TABLE x (s SYMBOL INDEX REPLICA ONLY, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO x VALUES ('a', 0), ('b', 86_400_000_000)");
            Assert.assertTrue(ReplicaOnlyIndexTestUtils.indexFilesExist(engine, "x", "s"));

            skip = true;
            engine.bumpRoleGeneration();
            failPurge.set(true);
            final TableToken token = engine.verifyTableName("x");
            boolean hasFailed = false;
            try (io.questdb.cairo.TableWriter writer = engine.getWriter(token, "partial purge")) {
                writer.reconcileReplicaOnlyIndexesIfRoleChanged();
            } catch (io.questdb.cairo.CairoException e) {
                hasFailed = true;
            }
            Assert.assertTrue("injected unlink failure must fail reconciliation", hasFailed);
            Assert.assertTrue("failed purge must leave the injected key file", ReplicaOnlyIndexTestUtils.indexFilesExist(engine, "x", "s"));
            Assert.assertFalse("failure injection must fire exactly once", failPurge.get());

            try (io.questdb.cairo.TableWriter writer = engine.getWriter(token, "partial purge retry")) {
                writer.reconcileReplicaOnlyIndexesIfRoleChanged();
            }
            Assert.assertFalse(
                    "retry must remove residual files even though the index is already incomplete",
                    ReplicaOnlyIndexTestUtils.indexFilesExist(engine, "x", "s")
            );
        });
    }

    @Test
    public void testRetainedFactoryIsRejectedAfterRoleChange() throws Exception {
        assertMemoryLeak(() -> {
            skip = false;
            execute("CREATE TABLE x (s SYMBOL INDEX REPLICA ONLY, v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO x VALUES ('a', 1, 0), ('b', 2, 1_000_000)");
            assertIndexUsed();

            final String query = "SELECT s, v, ts FROM x WHERE s = 'a'";
            try (
                    SqlCompiler compiler = engine.getSqlCompiler();
                    RecordCursorFactory factory = compiler.compile(query, sqlExecutionContext).getRecordCursorFactory()
            ) {
                skip = true;
                engine.bumpRoleGeneration();
                final TableToken token = engine.verifyTableName("x");
                try (io.questdb.cairo.TableWriter writer = engine.getWriter(token, "role change")) {
                    writer.reconcileReplicaOnlyIndexesIfRoleChanged();
                }

                boolean hasRejectedStaleFactory = false;
                try (RecordCursor ignored = factory.getCursor(sqlExecutionContext)) {
                    Assert.fail("factory compiled for the old role must be rejected");
                } catch (TableReferenceOutOfDateException e) {
                    hasRejectedStaleFactory = true;
                }
                Assert.assertTrue(hasRejectedStaleFactory);
            }

            // Standard query callers catch TableReferenceOutOfDateException and compile a fresh
            // full-scan plan. Verify that the fresh plan runs after the replica-only sidecars were purged.
            assertIndexNotUsed();
            assertContents("s\tv\tts\n" +
                    "a\t1.0\t1970-01-01T00:00:00.000000Z\n");
        });
    }

    @Test
    public void testRoleChangeDuringSharedCteCompilationRetriesCleanly() throws Exception {
        assertMemoryLeak(() -> {
            skip = false;
            execute("CREATE TABLE x (s SYMBOL INDEX REPLICA ONLY, v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO x VALUES ('a', 1, 0), ('b', 2, 1_000_000)");

            final AtomicInteger planCount = new AtomicInteger();
            final AtomicInteger prePlanCount = new AtomicInteger();
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                compiler.setRoleGenerationPrePlanObserver(cacheSize -> {
                    final int attempt = prePlanCount.incrementAndGet();
                    Assert.assertEquals(
                            "shared-factory cache must be empty at the start of compile attempt " + attempt,
                            0,
                            cacheSize
                    );
                });
                compiler.setRoleGenerationPlanObserver(cacheSize -> {
                    Assert.assertTrue("plan must populate the shared-factory cache", cacheSize > 0);
                    if (planCount.incrementAndGet() == 1) {
                        skip = true;
                        engine.bumpRoleGeneration();
                    }
                });
                try {
                    final String query = "SELECT o.s, o.total, sub.v " +
                            "FROM (SELECT s, sum(v) total FROM x GROUP BY s) o " +
                            "JOIN LATERAL (SELECT v FROM x WHERE v <= o.total) sub";
                    try (RecordCursorFactory factory = compiler.compile(query, sqlExecutionContext).getRecordCursorFactory();
                         RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        Assert.assertEquals("role change must trigger exactly one compile retry", 2, planCount.get());
                        Assert.assertEquals("both compile attempts must start with an empty shared cache", 2, prePlanCount.get());
                        int rowCount = 0;
                        while (cursor.hasNext()) {
                            rowCount++;
                        }
                        Assert.assertEquals(3, rowCount);
                    }
                } finally {
                    compiler.setRoleGenerationPlanObserver(null);
                    compiler.setRoleGenerationPrePlanObserver(null);
                }
            }
        });
    }

    // Query correctness over the index scan path: s = 'a' rows in timestamp order.
    private void assertContents(String expected) throws Exception {
        assertQuery("select s, v, ts from x where s = 'a'")
                .noLeakCheck().sizeMayVary().inferRandomAccess().inferTimestamp().returns(expected);
    }

    // On a non-skipping node the planner chooses a symbol index scan for s = 'a'
    // (the DeferredSingleSymbolFilterPageFrame factory), so the index is actually used.
    private void assertIndexUsed() throws Exception {
        assertQuery("select s, v, ts from x where s = 'a'")
                .noLeakCheck()
                .assertsPlanContaining("DeferredSingleSymbolFilterPageFrame");
    }

    // On a skipping primary the planner treats the column as un-indexed and must full-scan instead.
    private void assertIndexNotUsed() throws Exception {
        assertQuery("select s, v, ts from x where s = 'a'")
                .noLeakCheck()
                .assertsPlanNotContaining("Index forward scan", "Index backward scan", "DeferredSingleSymbolFilterPageFrame");
    }

    // C2: ATTACH on a NON-skipping node of an older partition that lacks its replica-only index
    // sidecars (a backup produced by a skipping primary) must REBUILD the index synchronously before
    // publishing -- not silently publish an index-less partition. Without the post-rename rebuild the
    // index-scan query below throws "replica-only index not materialized" for the reattached partition.
    @Test
    public void testNonSkippingAttachRebuildsMissingOlderPartitionIndex() throws Exception {
        assertMemoryLeak(() -> {
            skip = false; // non-skipping node: the index IS materialized and used
            execute("create table x (s symbol index capacity 32 replica only, v double, ts timestamp) timestamp(ts) partition by day bypass wal");
            // day0 (older) + day1 (current)
            execute("insert into x values ('a', 1, 0), ('b', 2, 3_600_000_000), ('a', 3, 86_400_000_000)");
            engine.releaseAllWriters();
            Assert.assertTrue("index files must exist on a non-skipping node", ReplicaOnlyIndexTestUtils.indexFilesExist(engine, "x", "s"));

            execute("alter table x detach partition list '1970-01-01'");
            engine.releaseAllWriters();

            // Simulate a partition copied from a skipping primary: strip its index sidecars AND its
            // _dmeta/_dcv so ATTACH reaches attachValidateMetadata and the sidecar-tolerance gate.
            deleteDetachedPartitionIndexFiles("x", "s", "1970-01-01");
            final TableToken token = engine.verifyTableName("x");
            try (Path p = new Path()) {
                p.of(engine.getConfiguration().getDbRoot()).concat(token).concat("1970-01-01").put(TableUtils.DETACHED_DIR_MARKER).concat(TableUtils.META_FILE_NAME).$();
                Assert.assertTrue("remove _dmeta", TestUtils.remove(p.$()));
                p.parent().concat(TableUtils.COLUMN_VERSION_FILE_NAME).$();
                Assert.assertTrue("remove _dcv", TestUtils.remove(p.$()));
            }
            renameDetachedToAttachable("x", "1970-01-01");

            execute("alter table x attach partition list '1970-01-01'");
            engine.releaseAllWriters();

            // The reattached older partition must now carry a rebuilt index...
            Assert.assertTrue(
                    "non-skipping ATTACH must rebuild the missing replica-only index for the older partition",
                    partitionIndexFilesExist("x", "s", "1970-01-01")
            );
            // ...and be transparently index-scannable together with the current partition (a still-missing
            // sidecar would make the reader throw "not materialized" here).
            assertMetadataFlagsKept();
            assertIndexUsed();
            assertContents("s\tv\tts\n" +
                    "a\t1.0\t1970-01-01T00:00:00.000000Z\n" +
                    "a\t3.0\t1970-01-02T00:00:00.000000Z\n");
        });
    }

    private boolean partitionIndexFilesExist(String table, String col, String partitionDir) {
        final boolean[] found = {false};
        ReplicaOnlyIndexTestUtils.forEachIndexFile(engine, table, col, (ff, fullPath) -> {
            final String s = fullPath.toString();
            // tolerate an optional partition-name txn suffix ("1970-01-01" or "1970-01-01.N"); the
            // detached/attachable staging dirs are already consumed by a successful ATTACH.
            if (Chars.contains(s, partitionDir) && !Chars.contains(s, TableUtils.DETACHED_DIR_MARKER)) {
                found[0] = true;
            }
        });
        return found[0];
    }

    private void deleteDetachedPartitionIndexFiles(String table, String col, String partitionDir) {
        final String detachedSegment = "/" + partitionDir + TableUtils.DETACHED_DIR_MARKER + "/";
        ReplicaOnlyIndexTestUtils.forEachIndexFile(engine, table, col, (ff, fullPath) -> {
            if (Chars.contains(fullPath.toString(), detachedSegment)) {
                ff.removeQuiet(fullPath.$());
            }
        });
    }

    private void renameDetachedToAttachable(String table, String partition) {
        final TableToken token = engine.verifyTableName(table);
        try (Path from = new Path(); Path to = new Path()) {
            from.of(engine.getConfiguration().getDbRoot()).concat(token).concat(partition).put(TableUtils.DETACHED_DIR_MARKER).$();
            to.of(engine.getConfiguration().getDbRoot()).concat(token).concat(partition).put(engine.getConfiguration().getAttachPartitionSuffix()).$();
            Assert.assertTrue(Files.rename(from.$(), to.$()) > -1);
        }
    }

    // After a role flip the indexed/replicaOnly metadata flags must remain set (only the on-disk
    // materialization is node-local).
    private void assertMetadataFlagsKept() {
        final TableToken token = engine.verifyTableName("x");
        try (io.questdb.cairo.TableReader reader = engine.getReader(token)) {
            final int colIdx = reader.getMetadata().getColumnIndex("s");
            Assert.assertTrue("column must remain flagged indexed in metadata", reader.getMetadata().isColumnIndexed(colIdx));
            Assert.assertTrue("column must remain flagged replicaOnly in metadata", reader.getMetadata().isColumnReplicaOnlyIndex(colIdx));
        }
    }

}
