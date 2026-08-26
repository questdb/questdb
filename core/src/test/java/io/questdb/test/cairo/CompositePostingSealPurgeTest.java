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

import io.questdb.griffin.SqlException;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * POSTING seal-purge on a composite table.
 * <p>
 * {@code PostingSealPurgeOperator} builds its partition path with the bare 5-arg
 * {@code setPathForNativePartition} -- {@code (timestampType, partitionBy, partitionTimestamp,
 * partitionNameTxn)}, no cell segment -- and the class carries no composite gate at all. POSTING
 * indexes ARE supported on composite tables, so the operator is reachable there, and it DELETES
 * superseded {@code .pv} / {@code .pc<N>} files.
 * <p>
 * This is the last entry in the native-file sweep: the same cell-blind-path family that produced the
 * squash, convert, reindex and drop-index defects on this branch. What matters is which of the two
 * outcomes it produces -- superseded posting files never reclaimed (a leak, as with the column-version
 * purge), or a live cell's posting files deleted (data loss).
 * <p>
 * The rows and the INDEXED reads are asserted against the plain twin at every stage, so a purge that
 * removed a live posting file shows up as a query divergence rather than as a silent size change.
 * <p>
 * <b>Scope, measured.</b> Repeated commits do NOT supersede a seal -- the index reseals
 * {@code .pv.0} in place -- so this workload never queues a seal-purge candidate and
 * {@code PostingSealPurgeOperator} never runs. What is established here is that indexed reads on a
 * composite table stay correct across repeated reseals, and that the per-cell posting layout is what
 * it should be. The operator's cell-blindness remains UNEXERCISED; see the comment at the end of the
 * test for what a workload reaching it would need to do.
 */
public class CompositePostingSealPurgeTest extends AbstractCompositeTwinTest {

    /**
     * Repeated commits into an indexed composite table reseal the posting index, superseding earlier
     * {@code .pv} / {@code .pc} files and queueing them for the seal purge.
     */
    @Test(timeout = 120_000)
    public void testResealPurgeKeepsIndexedReadsCorrect() throws Exception {
        assertMemoryLeak(() -> {
            createIndexedTwins();

            // Repeated commits into the SAME day and the SAME cell, so that cell's posting index is
            // resealed over and over and the earlier seals are superseded. An earlier version of this
            // test spread the inserts across days and cells: every cell then held exactly one
            // sym.pv.0, no seal was ever superseded, and the purge was never exercised at all.
            for (int i = 1; i <= 8; i++) {
                insertIntoBoth("('2023-01-01T0" + (i % 10) + ":00:00.000000Z','E0','S" + (i % 3) + "',"
                        + (i * 1.5) + ")");
                drainWalQueue();
            }
            engine.releaseInactive();

            // Unindexed and INDEXED reads must both agree with the plain twin. The indexed read is the
            // one that touches the posting files a bad purge would have deleted.
            assertTwinEqual("");
            assertTwinEqual(" WHERE sym = 'S1'");
            assertTwinEqual(" WHERE sym = 'S2'");

            // Then force the writer to release its indexers, which drains the seal-purge outbox.
            engine.releaseAllWriters();
            engine.releaseAllReaders();

            assertTwinEqual("");
            assertTwinEqual(" WHERE sym = 'S1'");

            // Structural: the composite table must carry PER-CELL posting files, not only the
            // day-level phantoms. Without this the indexed assertions above could pass on a table
            // whose index lived entirely at the day level, which is not the layout under test.
            Assert.assertTrue(
                    "precondition: the composite table must have per-cell posting files. Found: "
                            + postingFiles("c"),
                    postingFiles("c").stream().anyMatch(f -> f.contains("/E0")));
            Assert.assertFalse(
                    "precondition: the plain twin must have posting files too",
                    postingFiles("p").isEmpty());

            // WHAT THIS TEST DOES NOT COVER, stated so nobody reads it as purge coverage.
            // MEASURED after 8 sequential commits into one day and one cell:
            //     c = [2023-01-01/E0/sym.pk, 2023-01-01/E0/sym.pv.0,
            //          2023-01-01/sym.pk,    2023-01-01/sym.pv.0]
            //     p = [2023-01-01/sym.pk,    2023-01-01/sym.pv.0]
            // Exactly ONE seal version per side: the index reseals .pv.0 IN PLACE and never
            // supersedes it, so no seal-purge candidate is ever produced and
            // PostingSealPurgeOperator does not run. Its cell-blind path -- the bare 5-arg
            // setPathForNativePartition, with no composite gate in the class -- is therefore
            // UNEXERCISED here, not shown to be correct. Reaching it needs a workload that
            // supersedes a seal (a rewrite of an already-sealed partition: O3 into a sealed day,
            // reindex, or a parquet reseal), which is the next thing to try.
            Assert.assertEquals(
                    "the index still reseals in place; if superseded .pv.<N> versions now appear, the "
                            + "seal purge IS reachable here and this test should assert its reclaim. "
                            + "Found: " + postingFiles("c"),
                    2, postingFiles("c").stream().filter(f -> f.contains(".pv.")).count());
        });
    }

    /**
     * Posting-index files, table-relative. Real names are {@code sym.pv.<N>} and {@code sym.pk} -- an
     * earlier version of this filter looked for {@code ".pv"}/{@code ".pc<N>"} and silently counted
     * zero, which made the whole test pass on no evidence.
     */
    private List<String> postingFiles(String table) throws IOException {
        final List<String> out = new ArrayList<>();
        final Path base = tableDir(table);
        try (Stream<Path> all = Files.walk(base)) {
            all.filter(Files::isRegularFile)
                    .map(f -> base.relativize(f).toString())
                    .filter(n -> n.contains(".pv.") || n.endsWith(".pk") || n.contains(".pc"))
                    .sorted()
                    .forEach(out::add);
        }
        return out;
    }

    private void createIndexedTwins() throws SqlException {
        execute("CREATE TABLE c (ts TIMESTAMP, exch SYMBOL, sym SYMBOL INDEX TYPE POSTING, px DOUBLE) "
                + "TIMESTAMP(ts) PARTITION BY DAY, exch LAYOUT PLAIN WAL");
        execute("CREATE TABLE p (ts TIMESTAMP, exch SYMBOL, sym SYMBOL INDEX TYPE POSTING, px DOUBLE) "
                + "TIMESTAMP(ts) PARTITION BY DAY WAL");
    }

    private Path tableDir(String table) throws IOException {
        final Path root = Paths.get(configuration.getDbRoot());
        try (Stream<Path> children = Files.list(root)) {
            final List<Path> matches = new ArrayList<>();
            children.filter(Files::isDirectory)
                    .filter(pp -> pp.getFileName().toString().startsWith(table + "~"))
                    .forEach(matches::add);
            if (matches.isEmpty()) {
                throw new AssertionError("no table directory for " + table);
            }
            return matches.get(0);
        }
    }
}
