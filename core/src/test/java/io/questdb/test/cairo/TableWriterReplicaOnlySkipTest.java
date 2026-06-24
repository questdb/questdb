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

import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
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

    @BeforeClass
    public static void setUpStatic() throws Exception {
        configurationFactory = (root, telemetry, overrides) ->
                new CairoTestConfiguration(root, telemetry, overrides) {
                    @Override
                    public boolean skipReplicaOnlyIndexes() {
                        return true;
                    }
                };
        AbstractCairoTest.setUpStatic();
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
                    indexFilesExist("x", "s")
            );
            assertMetadataFlags("x", "s");
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
                    indexFilesExist("x", "s"));
            assertMetadataFlags("x", "s");
            // Full-scan correctness: the skipped index must not change query results.
            sink.clear();
            printSql("select s, v, ts from x where s = 'a'", sink);
            io.questdb.test.tools.TestUtils.assertEquals(
                    "s\tv\tts\n" +
                            "a\t1.0\t1970-01-01T00:00:00.000000Z\n" +
                            "a\t3.0\t1970-01-01T00:00:02.000000Z\n" +
                            "a\t5.0\t1970-01-02T00:00:01.000000Z\n",
                    sink);
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
            Assert.assertFalse("no index files expected on skipping primary after insert", indexFilesExist("x", "s"));

            // Round-trip the partition through parquet and back to native.
            execute("alter table x convert partition to parquet where ts >= 0");
            drainWalQueue();
            execute("alter table x convert partition to native where ts >= 0");
            drainWalQueue();

            // The conversion back to native must not have rebuilt the replica-only index.
            Assert.assertFalse(
                    "no index files expected on skipping primary after parquet->native conversion",
                    indexFilesExist("x", "s")
            );
            assertMetadataFlags("x", "s");

            // Full-scan correctness: the skipped index must not change query results.
            sink.clear();
            printSql("select s, ts from x where s = 'a'", sink);
            io.questdb.test.tools.TestUtils.assertEquals(
                    "s\tts\n" +
                            "a\t1970-01-01T00:00:00.000000Z\n" +
                            "a\t1970-01-01T00:00:02.000000Z\n",
                    sink);
        });
    }

    @Test
    public void testPrimarySkipsReplicaOnlyIndexBuild() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (s symbol index capacity 256 replica only, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into x values ('a', 0), ('b', 1000000), ('a', 2000000)");
            drainWalQueue();

            Assert.assertFalse("no index files expected on skipping primary", indexFilesExist("x", "s"));
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
            Assert.assertFalse("no index files expected on skipping primary before REINDEX", indexFilesExist("x", "s"));

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
            Assert.assertFalse("no index files expected on skipping primary after REINDEX", indexFilesExist("x", "s"));
            assertMetadataFlags("x", "s");
        });
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

    // Scans every partition directory under the table root for any bitmap key file
    // ("<col>.k.*"), bitmap value file ("<col>.v.*"), or posting index file
    // ("<col>.pk.*" / "<col>.pv.*") for the given column. Returns true if any exist.
    // Note: per-column index files are named "<col>.k"/"<col>.v" in the PARTITION dir; the symbol
    // dictionary's own "<col>.k"/"<col>.v" live at the TABLE ROOT and are deliberately not scanned here.
    private boolean indexFilesExist(String table, String col) {
        final TableToken token = engine.verifyTableName(table);
        final FilesFacade ff = engine.getConfiguration().getFilesFacade();
        final boolean[] found = {false};
        final StringSink fileName = new StringSink();
        final String keyPrefix = col + ".k";
        final String valPrefix = col + ".v";
        final String postingKeyPrefix = col + ".pk";
        final String postingValPrefix = col + ".pv";
        try (Path tablePath = new Path(); Path partPath = new Path()) {
            tablePath.of(engine.getConfiguration().getDbRoot()).concat(token.getDirName());
            ff.iterateDir(tablePath.$(), (pUtf8NameZ, type) -> {
                if (type != Files.DT_DIR) {
                    return;
                }
                fileName.clear();
                Utf8s.utf8ToUtf16Z(pUtf8NameZ, fileName);
                // skip "." and ".." plus non-partition dirs (wal*, txn_seq, etc.)
                if (Chars.equals(fileName, '.') || Chars.equals(fileName, "..") || Chars.startsWith(fileName, "wal") || Chars.startsWith(fileName, "txn_seq")) {
                    return;
                }
                partPath.of(engine.getConfiguration().getDbRoot()).concat(token.getDirName()).concat(fileName);
                final StringSink inner = new StringSink();
                ff.iterateDir(partPath.$(), (pInnerZ, innerType) -> {
                    if (innerType != Files.DT_FILE && innerType != Files.DT_UNKNOWN) {
                        return;
                    }
                    inner.clear();
                    Utf8s.utf8ToUtf16Z(pInnerZ, inner);
                    // exact "<col>.k" / "<col>.v" or with a columnNameTxn suffix "<col>.k.N"
                    if (matchesIndexFile(inner, postingKeyPrefix)
                            || matchesIndexFile(inner, postingValPrefix)
                            || matchesIndexFile(inner, keyPrefix)
                            || matchesIndexFile(inner, valPrefix)) {
                        found[0] = true;
                    }
                });
            });
        }
        return found[0];
    }

    // True if name == prefix, or name == prefix + "." + <digits> (the columnNameTxn-suffixed form).
    private boolean matchesIndexFile(CharSequence name, String prefix) {
        if (!Chars.startsWith(name, prefix)) {
            return false;
        }
        if (name.length() == prefix.length()) {
            return true;
        }
        // next char after the prefix must be '.' (txn suffix) to avoid matching e.g. "s.kx"
        return name.charAt(prefix.length()) == '.';
    }
}
