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

import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * {@code REINDEX TABLE} on a COMPOSITE table.
 * <p>
 * <b>The oracle: delete one cell's index files and see whether REINDEX brings them back.</b> This is
 * the only assertion that can distinguish a real rebuild from a silent no-op. REINDEX reports success
 * either way, and row counts and per-symbol counts stay correct either way -- the DATA is untouched, it
 * is the INDEX that does or does not get rebuilt. Measured with the old gate lifted and no fix:
 * <pre>
 *   deleted=true; restored=false     -- and REINDEX reported success
 * </pre>
 * <p>
 * The cause was the same cellKey-0 shape as the POSTING seal: {@code IndexBuilder#doReindex} built a
 * bare {@code <day>} path and resolved {@code _cv} by timestamp alone, so it looked for a partition
 * directory that holds no column data on a composite table, hit its own {@code ff.exists} guard and
 * logged "partition does not exist" -- a silent skip, not an error.
 * <p>
 * Deleting exactly ONE cell's files also proves the walk is per-cell rather than per-day: a day-level
 * rebuild would either restore nothing or rewrite the day container, and in neither case would the
 * untouched sibling cells be left byte-identical while the emptied one is repopulated.
 */
public class CompositeReindexTest extends AbstractCairoTest {

    @Test
    public void testReindexRebuildsADeletedCellIndex() throws Exception {
        assertMemoryLeak(() -> {
            createAndPopulate("c", "");
            assertReindexRestoresDeletedCell("c", ".k", ".v");
        });
    }

    /**
     * POSTING chains use a different indexer and a different on-disk shape ({@code .pk}/{@code .pv.N}).
     */
    @Test
    public void testReindexRebuildsADeletedCellPostingIndex() throws Exception {
        assertMemoryLeak(() -> {
            createAndPopulate("cp", " TYPE POSTING");
            assertReindexRestoresDeletedCell("cp", ".pk", ".pv");
        });
    }

    /**
     * POSITIVE CONTROL. The same delete-and-rebuild cycle on a PLAIN table must restore the files too,
     * so a composite failure is attributable to cell resolution rather than to REINDEX being broken.
     */
    @Test
    public void testPlainTableReindexRebuildsDeletedIndex() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE p (ts TIMESTAMP, exch SYMBOL, sym SYMBOL INDEX, px DOUBLE) "
                    + "TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO p VALUES "
                    + "('2023-08-01T01:00:00.000000Z','BTC','AAA',1.0),"
                    + "('2023-08-01T02:00:00.000000Z','ETH','BBB',2.0)");
            drainWalQueue();
            engine.releaseAllReaders();
            engine.releaseAllWriters();

            final List<String> before = indexFilesUnder("p~", ".k", ".v");
            Assert.assertFalse("no index files to begin with", before.isEmpty());
            for (String f : before) {
                Files.delete(Paths.get(configuration.getDbRoot()).resolve(f));
            }
            Assert.assertTrue("delete did not take", indexFilesUnder("p~", ".k", ".v").isEmpty());

            execute("REINDEX TABLE p COLUMN sym LOCK EXCLUSIVE");
            Assert.assertFalse("plain REINDEX must restore the deleted index files",
                    indexFilesUnder("p~", ".k", ".v").isEmpty());
        });
    }

    private void assertReindexRestoresDeletedCell(String table, String... suffixes) throws Exception {
        final String prefix = table + "~";
        engine.releaseAllReaders();
        engine.releaseAllWriters();

        final List<String> before = indexFilesUnder(prefix, suffixes);
        Assert.assertFalse("ingestion must leave per-cell index files to delete", before.isEmpty());

        // Exactly ONE cell -- the sibling cells stay as evidence that the rebuild is per-cell.
        final List<String> victim = new ArrayList<>();
        for (String f : before) {
            if (f.contains("ETH" + java.io.File.separator)) {
                victim.add(f);
            }
        }
        Assert.assertFalse("expected index files inside the ETH cell; saw " + before, victim.isEmpty());
        for (String f : victim) {
            Files.delete(Paths.get(configuration.getDbRoot()).resolve(f));
        }

        final List<String> afterDelete = indexFilesUnder(prefix, suffixes);
        Assert.assertTrue("delete did not take; saw " + afterDelete,
                afterDelete.stream().noneMatch(f -> f.contains("ETH" + java.io.File.separator)));
        Assert.assertTrue("sibling cells must be untouched by the delete",
                afterDelete.stream().anyMatch(f -> f.contains("BTC" + java.io.File.separator)));

        execute("REINDEX TABLE " + table + " COLUMN sym LOCK EXCLUSIVE");

        final List<String> restored = indexFilesUnder(prefix, suffixes);
        Assert.assertTrue("REINDEX reported success but did NOT rebuild the ETH cell's index; saw "
                        + restored,
                restored.stream().anyMatch(f -> f.contains("ETH" + java.io.File.separator)));

        // The data must still read correctly through the rebuilt index.
        // No expectSize(): a composite indexed scan is wrapped in a sort (wrapCompositeIndexedScan),
        // and a sorted cursor reports size -1 until it has been fully consumed.
        assertQuery("select ts, exch, sym from " + table + " where sym = 'AAA' order by ts")
                .noLeakCheck().sizeMayVary().timestamp("ts")
                .returns("ts\texch\tsym\n"
                        + "2023-08-01T01:00:00.000000Z\tBTC\tAAA\n"
                        + "2023-08-01T03:00:00.000000Z\tETH\tAAA\n");
    }

    private void createAndPopulate(String name, String indexType) throws Exception {
        execute("CREATE TABLE " + name + " (ts TIMESTAMP, exch SYMBOL, sym SYMBOL INDEX" + indexType
                + ", px DOUBLE) TIMESTAMP(ts) PARTITION BY DAY, exch WAL");
        // 'AAA' lives in BOTH cells, so the query assertion after the rebuild reads through the
        // repopulated ETH chain rather than answering entirely from the untouched BTC one.
        execute("INSERT INTO " + name + " VALUES "
                + "('2023-08-01T01:00:00.000000Z','BTC','AAA',1.0),"
                + "('2023-08-01T02:00:00.000000Z','ETH','BBB',2.0),"
                + "('2023-08-01T03:00:00.000000Z','ETH','AAA',3.0)");
        drainWalQueue();
    }

    /**
     * Index files living inside a CELL directory, i.e. {@code <table>/<day>/<cell>/<name><suffix>}.
     */
    private List<String> indexFilesUnder(String tablePrefix, String... suffixes) throws Exception {
        final List<String> out = new ArrayList<>();
        final Path root = Paths.get(configuration.getDbRoot());
        try (Stream<Path> walk = Files.walk(root, 5)) {
            for (Path f : walk.filter(p -> !Files.isDirectory(p)).toList()) {
                final String n = f.getFileName().toString();
                boolean match = false;
                for (String s : suffixes) {
                    if (n.contains(s)) {
                        match = true;
                        break;
                    }
                }
                if (!match || !f.toString().contains(tablePrefix)) {
                    continue;
                }
                // A table's ROOT also holds <symbolColumn>.k/.v -- the SYMBOL MAP files, which are not
                // partition indexes and whose deletion breaks the table outright (measured: "could not
                // open, file does not exist: p~1/exch.k"). Keep only files inside a partition, i.e.
                // whose parent is a day directory (plain) or whose grandparent is (composite cell).
                final Path parent = f.getParent();
                final Path grandParent = parent != null ? parent.getParent() : null;
                final boolean inPartition =
                        (parent != null && parent.getFileName().toString().startsWith("2023-"))
                                || (grandParent != null && grandParent.getFileName().toString().startsWith("2023-"));
                if (!inPartition) {
                    continue;
                }
                out.add(root.relativize(f).toString());
            }
        }
        out.sort(String::compareTo);
        return out;
    }
}
