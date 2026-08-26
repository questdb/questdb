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

import io.questdb.cairo.TableWriter;
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
 * Task 2b of the per-cell parquet plan: converting a composite day converts EVERY cell, each to its own
 * {@code <day>/<cell>.<txn>/data.parquet}.
 * <p>
 * Driven through {@code convertCompositePartitionToParquetForTest}, because the SQL-facing gate is
 * still closed -- until the cross-cell merge cursors read parquet cells (task 5), converting one would
 * make the table unreadable. So these assertions are deliberately STRUCTURAL: which files exist, and
 * what each cell's {@code _txn} record says. Reading the converted rows back is task 5's acceptance,
 * and is not claimed here.
 */
public class CompositePerCellParquetTest extends AbstractCompositeTwinTest {

    private static final String DAY = "2023-01-01";

    /**
     * Every cell converts, and each gets its own parquet file. A converter that resolved by timestamp
     * would convert only the first cell -- which is the defect that makes the ungated SQL path crash
     * the encoder -- so the assertion counts files per cell rather than merely checking that some
     * parquet appeared.
     */
    @Test(timeout = 60_000)
    public void testEveryCellOfTheDayBecomesItsOwnParquetFile() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            seedTwoCellDay();

            try (TableWriter w = getWriter("c")) {
                w.convertCompositePartitionToParquetForTest(parseFloorPartialTimestamp(DAY), null, 0.01);
            }

            final List<String> parquets = parquetFiles();
            Assert.assertEquals("both cells must have produced a parquet file, got " + parquets,
                    2, parquets.size());
            // and they must be nested per CELL, not at the bare day root
            for (int i = 0; i < parquets.size(); i++) {
                final String p = parquets.get(i);
                Assert.assertTrue("parquet must live under a cell directory, got " + p,
                        p.matches(DAY + "/E[01]\\.\\d+/data\\.parquet"));
            }
        });
    }

    /**
     * Invariant 3, and the reason PHASE 1 of the converter touches no {@code _txn} state: both cells'
     * records must flip together. Asserting the SECOND cell is what separates a per-cell converter from
     * one that did the first and stopped.
     */
    @Test(timeout = 60_000)
    public void testBothCellsAreMarkedParquetInTxn() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            seedTwoCellDay();

            try (TableWriter w = getWriter("c")) {
                w.convertCompositePartitionToParquetForTest(parseFloorPartialTimestamp(DAY), null, 0.01);
            }

            assertQuery("SELECT count() FROM table_partitions('c') WHERE isParquet = true")
                    .noLeakCheck().noRandomAccess().expectSize()
                    .returns("count\n2\n");
        });
    }

    /**
     * Seeds one day routed to TWO cells, plus a later day so the converted one is not the active
     * partition. Each commit is single-cell on purpose: an interleaved multi-cell commit has its own
     * unrelated gate on a table carrying a var-size column, and tripping it here would measure the
     * wrong thing.
     */
    private void seedTwoCellDay() throws Exception {
        insertIntoBoth("('" + DAY + "T01:00:00.000000Z','E0',1.0)");
        drainWalQueue();
        insertIntoBoth("('" + DAY + "T02:00:00.000000Z','E1',2.0)");
        drainWalQueue();
        insertIntoBoth("('2023-01-02T01:00:00.000000Z','E0',3.0)");
        drainWalQueue();
        engine.releaseInactive();
    }

    private List<String> parquetFiles() throws IOException {
        final Path root = tableDir();
        final List<String> out = new ArrayList<>();
        try (Stream<Path> w = Files.walk(root, 3)) {
            w.filter(p -> p.getFileName().toString().equals("data.parquet"))
                    .map(p -> root.relativize(p).toString())
                    .sorted()
                    .forEach(out::add);
        }
        return out;
    }

    private Path tableDir() throws IOException {
        try (Stream<Path> children = Files.list(Paths.get(configuration.getDbRoot()))) {
            return children.filter(Files::isDirectory)
                    .filter(pp -> pp.getFileName().toString().startsWith("c~"))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("no table directory for c"));
        }
    }
}
