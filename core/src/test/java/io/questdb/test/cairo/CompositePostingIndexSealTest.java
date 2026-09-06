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

import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * POSTING index on a COMPOSITE table, on a NON-DIMENSION symbol column.
 * <p>
 * Indexing a dimension column would be redundant -- the partitioning already provides that access
 * path -- so the case that matters is an ordinary symbol column alongside the dimension, which needs
 * its index per CELL, next to that cell's column data.
 * <p>
 * <b>What the write-side gate was, and what it was not.</b> {@code sealPostingIndexForPartition}
 * refused a routed composite table because it resolved every {@code _txn}/{@code _cv} lookup at
 * cellKey 0 and built a bare {@code <day>} path. It was NOT a sign that per-cell indexing was missing:
 * ingestion already writes {@code <day>/<cell>/sym.pk}. The {@code <day>/sym.pk} that also appears is
 * {@code openPartition}'s bare-container DEBRIS -- the same harmless artefact composite already leaves
 * for ordinary columns, a smaller and unused file. Mistaking it for the real index is what made this
 * look like a whole lifecycle migration.
 * <p>
 * <b>THE ORACLE: per-cell reseal evidence.</b> After an O3 commit that rewrites rowids in some cells
 * but not others, exactly the touched cells must carry a rotated {@code .pv} generation and the
 * untouched cell must not. A day-blind seal cannot produce that pattern -- it would rotate at the day
 * container, leaving every cell's generation untouched. Row counts alone would not distinguish the two,
 * because the DATA is correct either way; it is the INDEX that would be wrong.
 * <p>
 * NOT covered here: reading through the index. {@code composite partitioning does not yet support an
 * indexed WHERE predicate} still gates that, guarding a measured cell-major ORDER defect (a page-frame
 * scan walks cells sequentially -- see
 * {@code CompositeColumnDdlSurveyTest#surveyIndexedWhereReturnsCellMajorOrder}). This class should
 * grow an index-vs-scan assertion the day that lifts; until then the index is maintained correctly on
 * disk but not yet consulted by queries.
 */
public class CompositePostingIndexSealTest extends AbstractCairoTest {

    @Test(timeout = 120_000)
    public void testO3SealRotatesExactlyTheTouchedCells() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE p (ts TIMESTAMP, exch SYMBOL, sym SYMBOL INDEX TYPE POSTING, px DOUBLE) "
                    + "TIMESTAMP(ts) PARTITION BY DAY, exch LAYOUT PLAIN WAL");
            execute("INSERT INTO p VALUES "
                    + "('2023-07-01T02:00:00.000000Z','BTC','AAA',1.0),"
                    + "('2023-07-01T03:00:00.000000Z','ETH','BBB',2.0),"
                    + "('2023-07-01T04:00:00.000000Z','SOL','AAA',3.0),"
                    + "('2023-07-02T02:00:00.000000Z','BTC','CCC',4.0)");
            drainWalQueue();
            assertNotSuspended();

            // Every cell must already carry its own index -- if not, the reseal assertion below would
            // be meaningless.
            Assert.assertFalse("ingestion must write per-cell index files", cellIndexFiles().isEmpty());

            // O3 into BTC and ETH only, BEFORE existing rows, so those cells' rowids move and SOL's
            // do not. That asymmetry is the whole point: it distinguishes a per-cell seal from a
            // day-level one.
            execute("INSERT INTO p VALUES "
                    + "('2023-07-01T01:00:00.000000Z','BTC','AAA',5.0),"
                    + "('2023-07-01T01:30:00.000000Z','ETH','AAA',6.0)");
            drainWalQueue();
            assertNotSuspended();

            final List<String> after = cellIndexFiles();
            Assert.assertTrue("BTC took O3 rows, so its chain must have been resealed into a new .pv"
                            + " generation; saw " + after,
                    after.stream().anyMatch(f -> f.contains("/BTC") && f.endsWith("sym.pv.1")));
            Assert.assertTrue("ETH took O3 rows, so its chain must have been resealed; saw " + after,
                    after.stream().anyMatch(f -> f.contains("/ETH") && f.endsWith("sym.pv.1")));
            Assert.assertFalse("SOL took NO O3 rows, so it must NOT have been resealed -- a day-level"
                            + " seal would have rotated every cell or none; saw " + after,
                    after.stream().anyMatch(f -> f.contains("/SOL") && f.endsWith("sym.pv.1")));

            // Data intact. NO index is consulted here: the read path is still gated (see class doc).
            final StringSink sink = new StringSink();
            TestUtils.printSql(engine, sqlExecutionContext, "SELECT count() FROM p", sink);
            TestUtils.assertContains(sink, "6");
        });
    }

    /**
     * Index files living INSIDE a cell directory, i.e. {@code <day>/<cell>/<name>.pk|.pv.N}.
     */
    private List<String> cellIndexFiles() throws Exception {
        final List<String> out = new ArrayList<>();
        final Path root = Paths.get(configuration.getDbRoot());
        try (Stream<Path> walk = Files.walk(root, 5)) {
            for (Path f : walk.filter(p -> !Files.isDirectory(p)).toList()) {
                final String n = f.getFileName().toString();
                if (!n.contains(".pk") && !n.contains(".pv")) {
                    continue;
                }
                if (!f.toString().contains("p~")) {
                    continue;
                }
                final Path parent = f.getParent();
                // a CELL directory's parent is the day; the day's parent is the table
                if (parent != null && parent.getParent() != null
                        && parent.getParent().getFileName().toString().startsWith("2023-")) {
                    out.add(root.relativize(f).toString());
                }
            }
        }
        out.sort(String::compareTo);
        return out;
    }

    private void assertNotSuspended() {
        Assert.assertFalse("table suspended",
                engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("p")));
    }
}
