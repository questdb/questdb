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
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

/**
 * Checkpoint/restore of a composite table holding a PARQUET partition, or an INDEXED column.
 * <p>
 * Both were refused in {@code TableSnapshotRestore}, and both refusals are the same cell-blind-path
 * family as the rest of this branch:
 * <ul>
 *   <li>{@code processParquetPartition} builds its target with the cell-less
 *       {@code setPathForNativePartition}, so it looks for {@code <day>.<txn>/data.parquet} while the
 *       file lives at {@code <day>/<cell>.<txn>/};</li>
 *   <li>{@code rebuildBitmapIndexes} resolves each partition with the same bare 5-arg overload.</li>
 * </ul>
 * The parquet one only became reachable when CONVERT PARTITION TO PARQUET started working per cell --
 * the refusal's own comment records that premise correction. This is backup and restore, so a wrong
 * path here is a table that cannot be brought back.
 * <p>
 * <b>PINS BOTH REFUSALS, and records the blocker found by attempting the fix on 2026-08-28.</b> The
 * path-building half is easy and was written: both steps can iterate the partition's CELL directories
 * instead of the day directory. It is not sufficient, and the reason is worth writing down.
 * <p>
 * <b>The blocker: restore cannot map a partition RECORD to its DIRECTORY.</b> Both steps are dispatched
 * per attached-partition record, which on a composite table IS a cell, carrying that cell's own
 * name-txn, row count and committed parquet size. To use them it must know which directory the record
 * belongs to -- and it cannot:
 * <ul>
 *   <li>cell directories are named {@code <segment>.<nameTxn>}, and CONVERT stamps ONE name-txn across
 *       every cell of the day, so the suffix does not disambiguate {@code E0.5} from {@code E1.5};</li>
 *   <li>naming a cell from its cellKey needs {@code CellRegistry} plus a symbol reader per dimension --
 *       the interner stack {@code TableReader} builds at open -- and a restore deliberately works on
 *       raw files, before the table is openable.</li>
 * </ul>
 * Iterating directories per record instead is NOT a workaround: the index rebuild would apply one
 * record's row count and column top to a different cell's directory, producing a corrupt index. That
 * is worse than the refusal, which is why the attempt was reverted rather than committed.
 * <p>
 * <b>What a fix needs:</b> stand up the cell registry inside the restore -- a {@code SymbolMapReader}
 * over the table's {@code _cell} column (the LAST symbol column, which this class already relies on
 * for {@code isRoutedComposite}) wrapped in {@code CellRegistry}, plus a symbol reader per dimension
 * source column to turn each tuple ordinal into its segment name, honouring the spec's naming mode.
 * With that, {@code renderCellSegment}'s output identifies the directory and both steps become the
 * straightforward per-cell path change already prototyped.
 * <p>
 * Two things measured along the way, worth keeping: the restore's per-partition steps run
 * WORKER-PARALLEL, so any state a fix adds must be per task -- a shared list and name sink produced a
 * corrupted directory name and "index out of bounds, 1 >= 1"; and with the paths pointed at cell
 * directories the parquet step then failed on "restored parquet file is shorter than committed size",
 * which is the record-to-directory mismatch showing up as a size check rather than as a wrong path.
 */
public class CompositeCheckpointRestoreGapTest extends AbstractCairoTest {

    /**
     * A composite table with an INDEXED symbol column, restored with index rebuild ENABLED.
     * <p>
     * The rebuild is off by default ({@code cairo.checkpoint.recovery.rebuild.column.indexes}), so it
     * is turned on here -- otherwise the refusal is never reached and the test would pass vacuously.
     */
    @Test(timeout = 120_000)
    public void testRestoreCompositeWithIndexedColumn() throws Exception {
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_CHECKPOINT_RECOVERY_REBUILD_COLUMN_INDEXES, "true");
            setProperty(PropertyKey.CAIRO_LEGACY_SNAPSHOT_INSTANCE_ID, "id-before");

            execute("CREATE TABLE c (ts TIMESTAMP, exch SYMBOL, sym SYMBOL INDEX, px DOUBLE) TIMESTAMP(ts) "
                    + "PARTITION BY DAY, exch LAYOUT PLAIN WAL");
            execute("INSERT INTO c VALUES ('2023-01-01T01:00:00.000000Z','E0','S0',1.0),"
                    + "('2023-01-01T02:00:00.000000Z','E1','S1',2.0),"
                    + "('2023-01-02T01:00:00.000000Z','E0','S0',3.0)");
            drainWalQueue();

            final String scanBefore = capture("SELECT ts, exch, sym, px FROM c ORDER BY ts");
            final String indexedBefore = capture("SELECT ts, sym FROM c WHERE sym = 'S0' ORDER BY ts");

            execute("CHECKPOINT CREATE");
            // must NOT survive the restore
            execute("INSERT INTO c VALUES ('2023-01-03T01:00:00.000000Z','E1','S9',9.0)");
            drainWalQueue();

            engine.clear();
            setProperty(PropertyKey.CAIRO_LEGACY_SNAPSHOT_INSTANCE_ID, "id-after");
            try {
                engine.checkpointRecover();
                TestUtils.assertEquals("full scan must round-trip", scanBefore,
                        capture("SELECT ts, exch, sym, px FROM c ORDER BY ts"));
                // The INDEXED read is what exercises the rebuilt index.
                TestUtils.assertEquals("indexed read must round-trip", indexedBefore,
                        capture("SELECT ts, sym FROM c WHERE sym = 'S0' ORDER BY ts"));
            } finally {
                engine.checkpointRelease();
            }
        });
    }

    /**
     * A composite table with a PARQUET partition, restored.
     */
    @Test(timeout = 120_000)
    public void testRestoreCompositeWithParquetPartition() throws Exception {
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_LEGACY_SNAPSHOT_INSTANCE_ID, "id-before");

            execute("CREATE TABLE c (ts TIMESTAMP, exch SYMBOL, px DOUBLE) TIMESTAMP(ts) "
                    + "PARTITION BY DAY, exch LAYOUT PLAIN WAL");
            execute("INSERT INTO c VALUES ('2023-01-01T01:00:00.000000Z','E0',1.0),"
                    + "('2023-01-01T02:00:00.000000Z','E1',2.0),"
                    + "('2023-01-02T01:00:00.000000Z','E0',3.0)");
            drainWalQueue();
            execute("ALTER TABLE c CONVERT PARTITION TO PARQUET LIST '2023-01-01'");
            drainWalQueue();
            engine.releaseInactive();

            final String scanBefore = capture("SELECT ts, exch, px FROM c ORDER BY ts");
            final String day1Before = capture("SELECT ts, exch, px FROM c WHERE ts < '2023-01-02' ORDER BY ts");

            execute("CHECKPOINT CREATE");
            execute("INSERT INTO c VALUES ('2023-01-03T01:00:00.000000Z','E1',9.0)");
            drainWalQueue();

            engine.clear();
            setProperty(PropertyKey.CAIRO_LEGACY_SNAPSHOT_INSTANCE_ID, "id-after");
            try {
                engine.checkpointRecover();
                TestUtils.assertEquals("full scan must round-trip", scanBefore,
                        capture("SELECT ts, exch, px FROM c ORDER BY ts"));
                // The PARQUET day specifically -- the partition whose path had to resolve per cell.
                TestUtils.assertEquals("the parquet day must round-trip", day1Before,
                        capture("SELECT ts, exch, px FROM c WHERE ts < '2023-01-02' ORDER BY ts"));
            } finally {
                engine.checkpointRelease();
            }
        });
    }

    /**
     * An EXPRESSION dimension AND an indexed column, so the restore actually needs the resolver on the
     * DEDICATED-DICT branch.
     * <p>
     * Without this the branch is untested: an IDENTITY dimension resolves through its source column's
     * symbol map (the other branch), and a table with an expression dimension but no index or parquet
     * never opens the resolver at all now that it is lazy. It is also the branch that was WRONG -- the
     * dedicated dict's symbol count was read at the raw layout slot instead of
     * {@code dedicatedBase + slot}, tripping SymbolMapReaderImpl's charSize assertion.
     */
    @Test(timeout = 120_000)
    public void testRestoreCompositeWithExpressionDimensionAndIndex() throws Exception {
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_CHECKPOINT_RECOVERY_REBUILD_COLUMN_INDEXES, "true");
            setProperty(PropertyKey.CAIRO_LEGACY_SNAPSHOT_INSTANCE_ID, "id-before");

            execute("CREATE TABLE c (ts TIMESTAMP, exch SYMBOL, sym SYMBOL INDEX, px DOUBLE) TIMESTAMP(ts) "
                    + "PARTITION BY DAY, (upper(exch)) AS venue LAYOUT PLAIN WAL");
            execute("INSERT INTO c VALUES ('2023-01-01T01:00:00.000000Z','e0','S0',1.0),"
                    + "('2023-01-01T02:00:00.000000Z','E0','S1',2.0),"
                    + "('2023-01-01T03:00:00.000000Z','e1','S0',3.0),"
                    + "('2023-01-02T01:00:00.000000Z','e0','S1',4.0)");
            drainWalQueue();

            final String scanBefore = capture("SELECT ts, exch, sym, px FROM c ORDER BY ts");
            final String indexedBefore = capture("SELECT ts, sym FROM c WHERE sym = 'S0' ORDER BY ts");

            execute("CHECKPOINT CREATE");
            execute("INSERT INTO c VALUES ('2023-01-03T01:00:00.000000Z','e9','S9',9.0)");
            drainWalQueue();

            engine.clear();
            setProperty(PropertyKey.CAIRO_LEGACY_SNAPSHOT_INSTANCE_ID, "id-after");
            try {
                engine.checkpointRecover();
                TestUtils.assertEquals("full scan must round-trip", scanBefore,
                        capture("SELECT ts, exch, sym, px FROM c ORDER BY ts"));
                TestUtils.assertEquals("indexed read must round-trip", indexedBefore,
                        capture("SELECT ts, sym FROM c WHERE sym = 'S0' ORDER BY ts"));
            } finally {
                engine.checkpointRelease();
            }
        });
    }

    private String capture(String sql) throws Exception {
        sink.clear();
        printSql(sql);
        return sink.toString();
    }
}
