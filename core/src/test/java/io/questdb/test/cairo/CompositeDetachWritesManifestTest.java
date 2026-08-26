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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.CompositeCellManifest;
import io.questdb.cairo.CompositeDetachedArtifact;
import io.questdb.cairo.PartitionBy;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.stream.Stream;

/**
 * DETACH must leave a correct {@code _cell_manifest.d} in the artifact.
 * <p>
 * Distinct from {@code CompositeCellManifestTest}, which exercises the FORMAT against values the test
 * itself supplies. This drives a REAL detach and checks the manifest against the artifact's own
 * {@code _txn} -- the interesting failure is the two disagreeing about what the cells are, which a
 * synthetic round-trip cannot see.
 */
public class CompositeDetachWritesManifestTest extends AbstractCairoTest {

    @Test(timeout = 120_000)
    public void testDetachWritesManifestMatchingTxn() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE cm (ts TIMESTAMP, exch SYMBOL, px DOUBLE) TIMESTAMP(ts) "
                    + "PARTITION BY DAY, exch LAYOUT PLAIN WAL");
            // Three cells on the detached day, and a later day so it is not the ACTIVE partition.
            execute("INSERT INTO cm VALUES ('2023-04-01T01:00:00.000000Z','BTC',1.0),"
                    + "('2023-04-01T02:00:00.000000Z','ETH',2.0),"
                    + "('2023-04-01T03:00:00.000000Z','SOL',3.0),"
                    + "('2023-04-02T01:00:00.000000Z','BTC',4.0)");
            drainWalQueue();

            execute("ALTER TABLE cm DETACH PARTITION LIST '2023-04-01'");
            drainWalQueue();

            final java.nio.file.Path artifact = findArtifact("cm", "2023-04-01.detached");
            Assert.assertNotNull("detached artifact not found", artifact);
            Assert.assertTrue("DETACH must write a cell manifest into the artifact",
                    Files.exists(artifact.resolve(CompositeCellManifest.FILE_NAME)));

            final IntList manifestKeys = new IntList();
            final ObjList<String> values = new ObjList<>();
            final IntList txnKeys = new IntList();
            try (Path p = new Path()) {
                p.of(artifact.toString());
                final int dims = CompositeCellManifest.read(
                        configuration.getFilesFacade(), p, manifestKeys, values);
                Assert.assertEquals("one dimension in this table's spec", 1, dims);

                // The artifact's own _txn is the authority on which cells it holds. The manifest must
                // agree with it exactly -- not merely be well-formed.
                p.of(artifact.toString());
                CompositeDetachedArtifact.readCellKeys(
                        configuration.getFilesFacade(), p,
                        ColumnType.TIMESTAMP, PartitionBy.DAY,
                        io.questdb.std.datetime.microtime.Micros.DAY_MICROS * 19448L, txnKeys);
            }

            Assert.assertEquals("manifest must list exactly the cells _txn does",
                    txnKeys.size(), manifestKeys.size());
            Assert.assertEquals("one value per cell for a single-dimension table",
                    manifestKeys.size(), values.size());

            final HashSet<Integer> fromTxn = new HashSet<>();
            for (int i = 0; i < txnKeys.size(); i++) {
                fromTxn.add(txnKeys.getQuick(i));
            }
            final HashSet<Integer> fromManifest = new HashSet<>();
            for (int i = 0; i < manifestKeys.size(); i++) {
                fromManifest.add(manifestKeys.getQuick(i));
            }
            Assert.assertEquals("manifest cellKeys must match _txn's", fromTxn, fromManifest);

            // And the values must be the real dimension values, not path-encoded segments: a receiver
            // re-interns these verbatim, so "exch=BTC" or a %NULL token would file rows under a value
            // that never existed.
            final HashSet<String> seen = new HashSet<>();
            for (int i = 0; i < values.size(); i++) {
                seen.add(values.getQuick(i));
            }
            Assert.assertEquals("manifest must carry raw dimension values",
                    new HashSet<>(java.util.Arrays.asList("BTC", "ETH", "SOL")), seen);
        });
    }

    private static java.nio.file.Path findArtifact(String table, String child) throws Exception {
        final java.nio.file.Path root = Paths.get(configuration.getDbRoot());
        try (Stream<java.nio.file.Path> walk = Files.walk(root, 2)) {
            for (java.nio.file.Path p : walk.filter(Files::isDirectory).toList()) {
                if (p.getFileName().toString().equals(child)
                        && p.getParent().getFileName().toString().startsWith(table + "~")) {
                    return p;
                }
            }
        }
        return null;
    }
}
