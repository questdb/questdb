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
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Verifies Task 16: node-local reconcile of replica-only indexes on a role change, made
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

    private static volatile boolean skip;

    @BeforeClass
    public static void setUpStatic() throws Exception {
        skip = false;
        configurationFactory = (root, telemetry, overrides) ->
                new CairoTestConfiguration(root, telemetry, overrides) {
                    @Override
                    public boolean skipReplicaOnlyIndexes() {
                        return skip;
                    }
                };
        AbstractCairoTest.setUpStatic();
    }

    @Test
    public void testReconcileBuildsOnOpenWhenReplica() throws Exception {
        assertMemoryLeak(() -> {
            // Replica/standalone node (skip=false): the replica-only index is materialized on apply.
            skip = false;
            execute("create table x (s symbol index capacity 256 replica only, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into x values ('a',1,0),('b',2,1000000),('a',3,2000000)");
            drainWalQueue();
            Assert.assertTrue("replica-only index files must exist after WAL apply on a replica", indexFilesExist("x", "s"));

            // Simulate restore-from-primary-backup: the backup lacks the index sidecars. Delete them
            // and drop all writers so the next access reopens the writer and reconciles on open.
            engine.releaseAllWriters();
            deleteIndexFiles("x", "s");
            Assert.assertFalse("index files must be gone after simulated restore", indexFilesExist("x", "s"));

            // Force a writer reopen + apply. The constructor reconcile rebuilds the missing index;
            // the apply then maintains it for the new row.
            execute("insert into x values ('b',4,3000000)");
            drainWalQueue();

            Assert.assertTrue("reconcile-on-open must rebuild the replica-only index files", indexFilesExist("x", "s"));
            assertIndexUsed();
            assertContents("s\tv\tts\n" +
                    "a\t1.0\t1970-01-01T00:00:00.000000Z\n" +
                    "a\t3.0\t1970-01-01T00:00:02.000000Z\n");
        });
    }

    @Test
    public void testReconcileBuildsThenPurgesOnRoleFlip() throws Exception {
        assertMemoryLeak(() -> {
            // 1. Replica (skip=false): index materialized + used.
            skip = false;
            execute("create table x (s symbol index capacity 256 replica only, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into x values ('a',1,0),('b',2,1000000),('a',3,2000000)");
            drainWalQueue();
            Assert.assertTrue("index files must exist on a replica", indexFilesExist("x", "s"));
            assertIndexUsed();
            assertContents("s\tv\tts\n" +
                    "a\t1.0\t1970-01-01T00:00:00.000000Z\n" +
                    "a\t3.0\t1970-01-01T00:00:02.000000Z\n");

            // 2. Hot promotion to a skipping primary: flip the role, bump the generation, then an
            //    insert triggers the WAL-apply self-heal which PURGES the index sidecars.
            skip = true;
            engine.bumpRoleGeneration();
            execute("insert into x values ('a',4,4000000)");
            drainWalQueue();

            Assert.assertFalse("role flip to primary must purge the replica-only index files", indexFilesExist("x", "s"));
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
            execute("insert into x values ('b',5,5000000)");
            drainWalQueue();

            Assert.assertTrue("role flip back to replica must rebuild the replica-only index files", indexFilesExist("x", "s"));
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
            execute("insert into x values ('a',1,0),('b',2,1000000),('a',3,86400000000)"); // day0 + day1
            drainWalQueue();
            Assert.assertTrue("index files must exist on a replica", indexFilesExist("x", "s"));

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
                execute("insert into x values ('a',4,500000)"); // out-of-order within day0
                drainWalQueue();
                Assert.assertFalse("promote must purge the replica-only index files", indexFilesExist("x", "s"));

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
            execute("insert into n values ('a',1,0),('b',2,1000000),('a',3,86400000000)");
            drainWalQueue();
            Assert.assertTrue(indexFilesExist("n", "s"));

            final TableToken token = engine.verifyTableName("n");
            try (io.questdb.cairo.TableReader reader = engine.getReader(token)) {
                final int sIdx = reader.getMetadata().getColumnIndex("s");
                reader.openPartition(0);
                Assert.assertNotNull(reader.getIndexReader(0, sIdx, io.questdb.cairo.idx.IndexReader.DIR_BACKWARD));

                // Rewrite partition 0 (closes + reopens its columns), then delete the rebuilt index
                // files to simulate genuine corruption -- a normal index is never legitimately absent.
                execute("insert into n values ('a',4,500000)");
                drainWalQueue();
                deleteIndexFiles("n", "s");

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
            execute("insert into x values ('a',1,0),('b',2,1000000),('a',3,2000000)");
            drainWalQueue();
            Assert.assertTrue("index files must exist on a replica", indexFilesExist("x", "s"));

            final TableToken token = engine.verifyTableName("x");

            // Promote to a skipping primary, bump the generation, then drive the PUBLIC role-switch
            // sweep trigger directly on an already-open / pooled writer (no WAL apply): it must purge
            // the stale replica-only index sidecars.
            skip = true;
            engine.bumpRoleGeneration();
            try (io.questdb.cairo.TableWriter writer = engine.getWriter(token, "sweep")) {
                writer.reconcileReplicaOnlyIndexesIfRoleChanged();
            }
            Assert.assertFalse("role-switch sweep trigger must purge the replica-only index files", indexFilesExist("x", "s"));

            // Demote back to a replica, bump again, and trigger the sweep: it must rebuild the index.
            skip = false;
            engine.bumpRoleGeneration();
            try (io.questdb.cairo.TableWriter writer = engine.getWriter(token, "sweep")) {
                writer.reconcileReplicaOnlyIndexesIfRoleChanged();
                // Idempotent: a second call with no further gen bump is a cheap no-op.
                writer.reconcileReplicaOnlyIndexesIfRoleChanged();
            }
            Assert.assertTrue("role-switch sweep trigger must rebuild the replica-only index files", indexFilesExist("x", "s"));
            assertIndexUsed();
            assertContents("s\tv\tts\n" +
                    "a\t1.0\t1970-01-01T00:00:00.000000Z\n" +
                    "a\t3.0\t1970-01-01T00:00:02.000000Z\n");
        });
    }

    // Query correctness over the index scan path: s = 'a' rows in timestamp order.
    private void assertContents(String expected) throws Exception {
        sink.clear();
        printSql("select s, v, ts from x where s = 'a'", sink);
        TestUtils.assertEquals(expected, sink);
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

    private void deleteIndexFiles(String table, String col) {
        forEachIndexFile(table, col, (ff, fullPath) -> ff.removeQuiet(fullPath.$()));
    }

    private void forEachIndexFile(String table, String col, IndexFileAction action) {
        final TableToken token = engine.verifyTableName(table);
        final FilesFacade ff = engine.getConfiguration().getFilesFacade();
        final StringSink dirName = new StringSink();
        final String keyPrefix = col + ".k";
        final String valPrefix = col + ".v";
        final String postingKeyPrefix = col + ".pk";
        final String postingValPrefix = col + ".pv";
        try (Path tablePath = new Path(); Path partPath = new Path(); Path filePath = new Path()) {
            tablePath.of(engine.getConfiguration().getDbRoot()).concat(token.getDirName());
            ff.iterateDir(tablePath.$(), (pUtf8NameZ, type) -> {
                if (type != Files.DT_DIR) {
                    return;
                }
                dirName.clear();
                Utf8s.utf8ToUtf16Z(pUtf8NameZ, dirName);
                if (Chars.equals(dirName, '.') || Chars.equals(dirName, "..")
                        || Chars.startsWith(dirName, "wal") || Chars.startsWith(dirName, "txn_seq")) {
                    return;
                }
                partPath.of(engine.getConfiguration().getDbRoot()).concat(token.getDirName()).concat(dirName);
                final StringSink inner = new StringSink();
                ff.iterateDir(partPath.$(), (pInnerZ, innerType) -> {
                    if (innerType != Files.DT_FILE && innerType != Files.DT_UNKNOWN) {
                        return;
                    }
                    inner.clear();
                    Utf8s.utf8ToUtf16Z(pInnerZ, inner);
                    if (matchesIndexFile(inner, postingKeyPrefix)
                            || matchesIndexFile(inner, postingValPrefix)
                            || matchesIndexFile(inner, keyPrefix)
                            || matchesIndexFile(inner, valPrefix)) {
                        filePath.of(engine.getConfiguration().getDbRoot())
                                .concat(token.getDirName()).concat(dirName).concat(inner);
                        action.apply(ff, filePath);
                    }
                });
            });
        }
    }

    // True if any per-partition index file exists for the column (the symbol dictionary's own
    // "<col>.k"/"<col>.v" live at the TABLE ROOT and are deliberately not scanned here).
    private boolean indexFilesExist(String table, String col) {
        final boolean[] found = {false};
        forEachIndexFile(table, col, (ff, fullPath) -> found[0] = true);
        return found[0];
    }

    // True if name == prefix, or name == prefix + "." + <suffix> (the columnNameTxn-suffixed form).
    private boolean matchesIndexFile(CharSequence name, String prefix) {
        if (!Chars.startsWith(name, prefix)) {
            return false;
        }
        if (name.length() == prefix.length()) {
            return true;
        }
        return name.charAt(prefix.length()) == '.';
    }

    @FunctionalInterface
    private interface IndexFileAction {
        void apply(FilesFacade ff, Path fullPath);
    }
}
