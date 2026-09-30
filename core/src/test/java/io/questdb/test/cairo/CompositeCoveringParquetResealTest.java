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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.PostingSealPurgeOperator;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.std.str.StringSink;
import io.questdb.tasks.PostingSealPurgeTask;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A COVERING posting index resealed on a COMPOSITE day's parquet cell.
 * <p>
 * When O3 rewrites an already-parquet partition, the worker builds only the non-covering {@code .pv};
 * {@code resealParquetCoveringForPartition} rebuilds the covering sidecars from the new parquet before
 * the commit exposes it. That method refused a routed composite table outright, because every lookup
 * in it answered for cellKey 0 -- the partition record, the directory, the row count and the covered
 * columns' tops and name txns.
 * <p>
 * This is the one covering path whose covered column tops are read UN-NORMALIZED (the parquet re-encode
 * forces them to zero, which is why an enterprise re-encode test cannot distinguish a cellKey-0 read).
 * So the fixture makes the tops differ per cell -- E0 holds three pre-column rows, E1 one -- and the
 * oracle is the value the covering index returns for the SECOND cell.
 */
public class CompositeCoveringParquetResealTest extends AbstractCairoTest {

    /**
     * The NATIVE counterpart, and the one that pins the covered-column TOPS.
     * <p>
     * {@code ALTER COLUMN ... ADD INDEX} rebuilds each existing partition through
     * {@code indexNativePartition}, which resolves the cell for its own column top and size but
     * configured the COVERING columns at cellKey 0. A native partition keeps genuinely per-cell tops
     * (a parquet one carries zeros after conversion, which is why no parquet-backed test can see this),
     * so a sibling cell's covering was sealed over cell 0's covered data.
     * <p>
     * E0 holds three rows before the covered column exists and E1 one, so their tops differ (3 vs 1).
     * The oracle is the covered VALUE the index returns for each cell.
     */
    @Test
    public void testCompositeCellsPublishAndReadParquetFormCoveringIndexes() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_PARQUET_PARTITION_FORMAT, "parquet");
        assertMemoryLeak(() -> {
            execute("CREATE TABLE c (ts TIMESTAMP, exch SYMBOL, sym SYMBOL, val INT) TIMESTAMP(ts) "
                    + "PARTITION BY DAY, exch WAL");
            execute("ALTER TABLE c ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (val)");
            execute("INSERT INTO c VALUES "
                    + "('2023-01-01T01:00:00.000000Z','E0','A',10),"
                    + "('2023-01-01T02:00:00.000000Z','E1','A',20),"
                    + "('2023-01-02T01:00:00.000000Z','E0','A',30)");
            drainWalQueue();

            execute("ALTER TABLE c CONVERT PARTITION TO PARQUET LIST '2023-01-01'");
            drainWalQueue();

            assertQuery("SELECT exch, val FROM c WHERE sym = 'A' AND ts IN '2023-01-01' ORDER BY exch")
                    .noLeakCheck()
                    .returns("exch\tval\nE0\t10\nE1\t20\n");

            // Reseal only the second cell. Its token publish must address E1's
            // _pm, while the first cell keeps resolving its own pidx pair.
            execute("INSERT INTO c VALUES ('2023-01-01T01:30:00.000000Z','E1','A',21)");
            drainWalQueue();

            assertQuery("SELECT exch, val FROM c WHERE sym = 'A' AND ts IN '2023-01-01' ORDER BY exch, val")
                    .noLeakCheck()
                    .returns("exch\tval\nE0\t10\nE1\t20\nE1\t21\n");

            // Both cells' first parquet seals used the same table txn. Prove a
            // parquet purge addresses only its owning cell: put an orphan pair
            // with E0's still-live suffix in E1's current directory, then purge
            // that E1 pair. Enumerating siblings would unlink E0's live pair.
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            final TableToken tableToken = engine.verifyTableName("c");
            final Path tableRoot = Paths.get(configuration.getDbRoot().toString(), tableToken.getDirName());
            final List<Path> pidxFiles;
            try (java.util.stream.Stream<Path> stream = Files.walk(tableRoot)) {
                pidxFiles = stream
                        .filter(Files::isRegularFile)
                        .filter(p -> p.getFileName().toString().startsWith("sym.pidx."))
                        .filter(p -> p.getFileName().toString().endsWith(".parquet"))
                        .collect(Collectors.toList());
            }
            final Path e0Live = pidxFiles.stream()
                    .filter(p -> p.getParent().getFileName().toString().startsWith("exch=E0"))
                    .findFirst()
                    .orElseThrow();
            final Path e1Live = pidxFiles.stream()
                    .filter(p -> p.getParent().getFileName().toString().startsWith("exch=E1"))
                    .findFirst()
                    .orElseThrow();
            final String e0FileName = e0Live.getFileName().toString();
            final long sharedIndexTxn = Long.parseLong(
                    e0FileName.substring("sym.pidx.".length(), e0FileName.length() - ".parquet".length())
            );
            final Path e1OrphanParquet = e1Live.getParent().resolve(e0FileName);
            final Path e1OrphanIm = e1Live.getParent().resolve(
                    e0FileName.substring(0, e0FileName.length() - ".parquet".length()) + "._im"
            );
            Assert.assertFalse("fixture suffix must not already be live in E1", Files.exists(e1OrphanParquet));
            Files.createFile(e1OrphanParquet);
            Files.createFile(e1OrphanIm);

            int e1CellKey = -1;
            try (TableReader reader = engine.getReader(tableToken)) {
                final StringSink cellSegment = new StringSink();
                for (int i = 0, n = reader.getPartitionCount(); i < n; i++) {
                    cellSegment.clear();
                    final int cellKey = reader.getPartitionCellKey(i);
                    reader.renderCellSegment(cellSegment, cellKey);
                    if ("exch=E1".contentEquals(cellSegment)) {
                        e1CellKey = cellKey;
                        break;
                    }
                }
            }
            Assert.assertTrue("E1 cell key must resolve", e1CellKey >= 0);
            final String e1DirName = e1Live.getParent().getFileName().toString();
            final int nameTxnDot = e1DirName.lastIndexOf('.');
            final long e1PartitionNameTxn = nameTxnDot < 0 ? -1 : Long.parseLong(e1DirName.substring(nameTxnDot + 1));
            final PostingSealPurgeTask task = new PostingSealPurgeTask();
            task.of(
                    tableToken,
                    "sym",
                    e1CellKey,
                    sharedIndexTxn,
                    PostingSealPurgeTask.ARTIFACT_FORM_PARQUET,
                    1672531200000000L,
                    e1PartitionNameTxn,
                    PartitionBy.DAY,
                    ColumnType.TIMESTAMP_MICRO,
                    0,
                    1
            );
            try (PostingSealPurgeOperator operator = new PostingSealPurgeOperator(engine)) {
                Assert.assertTrue("the E1 orphan pair must be purgeable", operator.purge(task));
            }
            Assert.assertFalse("E1 orphan parquet must be removed", Files.exists(e1OrphanParquet));
            Assert.assertFalse("E1 orphan _im must be removed", Files.exists(e1OrphanIm));
            Assert.assertTrue("E0's same-txn live parquet must survive", Files.exists(e0Live));

            assertQuery("SELECT exch, val FROM c WHERE sym = 'A' AND ts IN '2023-01-01' ORDER BY exch, val")
                    .noLeakCheck()
                    .returns("exch\tval\nE0\t10\nE1\t20\nE1\t21\n");
        });
    }

    @Test
    public void testNativeCoveringIndexBuildReadsEachCellsOwnCoveredTops() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE n (ts TIMESTAMP, exch SYMBOL, sym SYMBOL) TIMESTAMP(ts) "
                    + "PARTITION BY DAY, exch WAL");
            execute("INSERT INTO n VALUES ('2023-01-01T01:00:00.000000Z','E0','A'),"
                    + "('2023-01-01T01:10:00.000000Z','E0','A'),"
                    + "('2023-01-01T01:20:00.000000Z','E0','B'),"
                    + "('2023-01-01T02:00:00.000000Z','E1','A'),"
                    + "('2023-01-02T01:00:00.000000Z','E0','A')");
            drainWalQueue();
            execute("ALTER TABLE n ADD COLUMN val INT");
            execute("INSERT INTO n VALUES ('2023-01-01T03:00:00.000000Z','E0','C',7),"
                    + "('2023-01-01T04:00:00.000000Z','E1','D',42)");
            drainWalQueue();
            // Built AFTER the rows exist, so every partition is indexed through indexNativePartition --
            // the path whose covering configuration was cell-blind.
            execute("ALTER TABLE n ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (val)");
            drainWalQueue();

            assertQuery("SELECT ts, sym, val FROM n WHERE sym = 'D'")
                    .noLeakCheck().timestamp("ts").expectSize()
                    .returns("ts\tsym\tval\n2023-01-01T04:00:00.000000Z\tD\t42\n");
            assertQuery("SELECT ts, sym, val FROM n WHERE sym = 'C'")
                    .noLeakCheck().timestamp("ts").expectSize()
                    .returns("ts\tsym\tval\n2023-01-01T03:00:00.000000Z\tC\t7\n");
            // The pre-column rows still read NULL through the index, per cell.
            assertQuery("SELECT ts, sym, val FROM n WHERE sym = 'B'")
                    .noLeakCheck().timestamp("ts").expectSize()
                    .returns("ts\tsym\tval\n2023-01-01T01:20:00.000000Z\tB\tnull\n");
        });
    }

    @Test
    public void testCoveringResealOnASiblingCellReadsItsOwnCoveredValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE c (ts TIMESTAMP, exch SYMBOL, sym SYMBOL) TIMESTAMP(ts) "
                    + "PARTITION BY DAY, exch WAL");
            // Different pre-existing row counts per cell: E0 three, E1 one. That is what makes the
            // covered column's top differ between the two cells once it is added below.
            execute("INSERT INTO c VALUES ('2023-01-01T01:00:00.000000Z','E0','A'),"
                    + "('2023-01-01T01:10:00.000000Z','E0','A'),"
                    + "('2023-01-01T01:20:00.000000Z','E0','B'),"
                    + "('2023-01-01T02:00:00.000000Z','E1','A'),"
                    + "('2023-01-02T01:00:00.000000Z','E0','A')");
            drainWalQueue();
            execute("ALTER TABLE c ADD COLUMN val INT");
            execute("ALTER TABLE c ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (val)");
            execute("INSERT INTO c VALUES ('2023-01-01T03:00:00.000000Z','E0','C',7),"
                    + "('2023-01-01T04:00:00.000000Z','E1','D',42)");
            drainWalQueue();

            execute("ALTER TABLE c CONVERT PARTITION TO PARQUET LIST '2023-01-01'");
            drainWalQueue();

            // O3 into the SECOND cell rewrites its parquet, and the rewrite is what drives the
            // covering reseal for that cell.
            execute("INSERT INTO c VALUES ('2023-01-01T02:30:00.000000Z','E1','D',99)");
            drainWalQueue();

            // The covering index must answer with E1's own covered values, not E0's.
            assertQuery("SELECT ts, sym, val FROM c WHERE sym = 'D' ORDER BY ts")
                    .noLeakCheck().timestamp("ts").expectSize()
                    .returns("ts\tsym\tval\n"
                            + "2023-01-01T02:30:00.000000Z\tD\t99\n"
                            + "2023-01-01T04:00:00.000000Z\tD\t42\n");
            // The first cell's own covered value is equally its own.
            assertQuery("SELECT ts, sym, val FROM c WHERE sym = 'C'")
                    .noLeakCheck().timestamp("ts").expectSize()
                    .returns("ts\tsym\tval\n2023-01-01T03:00:00.000000Z\tC\t7\n");
            // Rows that predate the covered column read NULL, per cell.
            assertQuery("SELECT count() FROM c").noLeakCheck().noRandomAccess().expectSize().returns("count\n8\n");
        });
    }
}
