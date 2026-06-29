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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Verifies the reader-side robustness of Task 13. This test runs with the DEFAULT configuration
 * ({@link io.questdb.cairo.CairoConfiguration#skipReplicaOnlyIndexes()} returns false), i.e. a
 * non-skipping node (replica/standalone). On such a node a replica-only indexed column is treated
 * as active by the planner, so an index scan may be chosen and the index reader opened.
 * <p>
 * There is a transient window -- e.g. immediately after restoring from a primary's backup that lacks
 * the index files, before reconcile rebuilds them -- where the column is flagged indexed (and
 * replica-only) but its {@code k./v.} files are ABSENT. For a REPLICA-ONLY column a missing index file
 * is NOT corruption -- it just means "not materialized here yet" -- and the reader must degrade
 * gracefully (recoverable / non-critical per-query error) instead of throwing a CRITICAL corruption
 * error that would suspend the table.
 * <p>
 * The second test pins down the invariant we must NOT weaken: for a NORMAL (non-replica-only) indexed
 * column, a missing index file remains genuine corruption -- a CRITICAL error.
 */
public class ReplicaOnlyIndexReaderToleranceTest extends AbstractCairoTest {

    @Test
    public void testMissingNormalIndexFilesAreStillCritical() throws Exception {
        assertMemoryLeak(() -> {
            // NORMAL indexed column (no "replica only"): missing index files must remain CRITICAL (corruption).
            execute("create table n (s symbol index capacity 256, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into n values ('a',1,0),('b',2,1000000),('a',3,2000000)");
            drainWalQueue();

            // sanity: this query uses a symbol index scan on a non-skipping node
            assertQuery("select s, v, ts from n where s = 'a'")
                    .noLeakCheck()
                    .assertsPlanContaining("DeferredSingleSymbolFilterPageFrame");

            Assert.assertTrue("normal index files must exist after WAL apply", indexFilesExist("n", "s"));
            deleteIndexFiles("n", "s");
            Assert.assertFalse("normal index files must be gone after delete", indexFilesExist("n", "s"));

            boolean threw = false;
            try {
                runQuery("select s, v, ts from n where s = 'a'");
                Assert.fail("expected a CairoException for a normal indexed column with missing index files");
            } catch (CairoException e) {
                threw = true;
                Assert.assertTrue("missing files for a NORMAL index must remain CRITICAL (corruption)", e.isCritical());
            }
            Assert.assertTrue(threw);
        });
    }

    @Test
    public void testMissingReplicaOnlyIndexFilesDegradeGracefully() throws Exception {
        assertMemoryLeak(() -> {
            // DEFAULT config => skipReplicaOnlyIndexes()==false => column is active and index gets built on WAL apply.
            execute("create table x (s symbol index capacity 256 replica only, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into x values ('a',1,0),('b',2,1000000),('a',3,2000000)");
            drainWalQueue();

            // sanity: on a non-skipping node the planner DOES choose a symbol index scan for s = 'a',
            // so the index reader is actually opened when the cursor runs.
            assertQuery("select s, v, ts from x where s = 'a'")
                    .noLeakCheck()
                    .assertsPlanContaining("DeferredSingleSymbolFilterPageFrame");

            // index files exist now; delete them to simulate the post-restore / pre-reconcile transient.
            Assert.assertTrue("replica-only index files must exist after WAL apply on a non-skipping node", indexFilesExist("x", "s"));
            deleteIndexFiles("x", "s");
            Assert.assertFalse("replica-only index files must be gone after delete", indexFilesExist("x", "s"));

            boolean threw = false;
            try {
                runQuery("select s, v, ts from x where s = 'a'");
                Assert.fail("expected a recoverable error for a replica-only index with missing files");
            } catch (CairoException e) {
                threw = true;
                Assert.assertFalse("must be recoverable (non-critical), not corruption", e.isCritical());
                TestUtils.assertContains(e.getFlyweightMessage(), "replica-only index not materialized");
            }
            Assert.assertTrue(threw);

            // table must still be usable for non-index queries -- it must NOT be suspended.
            sink.clear();
            printSql("select count(*) from x", sink);
            TestUtils.assertEquals("count\n3\n", sink);
        });
    }

    @Test
    public void testReplicaOnlyIndexOpenRaceIsRecoverable() throws Exception {
        // Race backstop for createIndexReaderAt: the cheap pre-check (ff.exists on the key file)
        // can PASS and then the file be unlinked by a concurrent reconcile-purge before the reader
        // ctor opens it, so the open itself fails with a critical "file does not exist" error.
        // For a replica-only column that must still degrade gracefully (non-critical), exactly like
        // the pre-check path. We simulate the race deterministically: the key file genuinely stays
        // on disk (so ff.exists() -> true, pre-check passes), but the FilesFacade is armed to fail
        // the open of that key file with ENOENT (so the ctor open fails). This drives the catch.
        final AtomicBoolean armed = new AtomicBoolean(false);
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public int errno() {
                // When armed and we just forced an open to fail, report "file does not exist" so
                // Files.isErrnoFileCannotRead() is true and the catch converts it to non-critical.
                return armed.get() ? CairoException.ERRNO_FILE_DOES_NOT_EXIST : super.errno();
            }

            @Override
            public long openRO(LPSZ name) {
                if (armed.get() && isReplicaOnlyKeyFile(name)) {
                    return -1;
                }
                return super.openRO(name);
            }

            @Override
            public long openRONoCache(LPSZ path) {
                if (armed.get() && isReplicaOnlyKeyFile(path)) {
                    return -1;
                }
                return super.openRONoCache(path);
            }

            // Match the bitmap index KEY file for column "s" ("s.k", optionally txn-suffixed).
            // The symbol dictionary's own root-level "s.k" is excluded because we only arm during
            // the per-partition index reader open, and matching the basename is sufficient here.
            private boolean isReplicaOnlyKeyFile(LPSZ name) {
                final String s = Utf8s.stringFromUtf8Bytes(name);
                final int slash = Math.max(s.lastIndexOf('/'), s.lastIndexOf('\\'));
                final String base = slash >= 0 ? s.substring(slash + 1) : s;
                return base.equals("s.k") || base.startsWith("s.k.");
            }
        };

        assertMemoryLeak(ff, () -> {
            execute("create table x (s symbol index capacity 256 replica only, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into x values ('a',1,0),('b',2,1000000),('a',3,2000000)");
            drainWalQueue();

            // sanity: the planner picks a symbol index scan, so the reader is opened on this query.
            assertQuery("select s, v, ts from x where s = 'a'")
                    .noLeakCheck()
                    .assertsPlanContaining("DeferredSingleSymbolFilterPageFrame");

            // The key files are present on disk: the cheap pre-check WILL pass.
            Assert.assertTrue("replica-only index key file must exist on disk", indexFilesExist("x", "s"));

            // Arm: pre-check still sees the file (exists==true), but the ctor open is forced to fail
            // with ENOENT -- the exact TOCTOU the catch must absorb.
            armed.set(true);
            boolean threw = false;
            try {
                runQuery("select s, v, ts from x where s = 'a'");
                Assert.fail("expected a recoverable error when the index open loses the unlink race");
            } catch (CairoException e) {
                threw = true;
                Assert.assertFalse("open-race failure must be recoverable (non-critical), not corruption", e.isCritical());
                TestUtils.assertContains(e.getFlyweightMessage(), "replica-only index not materialized");
            } finally {
                armed.set(false);
            }
            Assert.assertTrue(threw);

            // The table must not be suspended: non-index queries still work, and once disarmed the
            // index query succeeds again (the file was never actually removed).
            sink.clear();
            printSql("select count(*) from x", sink);
            TestUtils.assertEquals("count\n3\n", sink);
            runQuery("select s, v, ts from x where s = 'a'");
        });
    }

    // Drains the cursor of a query so the index reader is actually opened during execution.
    private static void runQuery(CharSequence sql) throws Exception {
        try (RecordCursorFactory factory = select(sql)) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                //noinspection StatementWithEmptyBody
                while (cursor.hasNext()) {
                    // drain
                }
            }
        }
    }

    // Deletes every per-partition bitmap/posting index file ("<col>.k"/"<col>.v"/"<col>.pk"/"<col>.pv",
    // with or without a columnNameTxn suffix) for the given column. Inverts indexFilesExist below.
    private void deleteIndexFiles(String table, String col) {
        forEachIndexFile(table, col, (ff, fullPath) -> ff.removeQuiet(fullPath.$()));
    }

    // True if any per-partition index file exists for the column (the symbol dictionary's own
    // "<col>.k"/"<col>.v" live at the TABLE ROOT and are deliberately not scanned here).
    private boolean indexFilesExist(String table, String col) {
        final boolean[] found = {false};
        forEachIndexFile(table, col, (ff, fullPath) -> found[0] = true);
        return found[0];
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
