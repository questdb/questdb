/*+*****************************************************************************
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
import io.questdb.cairo.ColumnVersionWriter;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.SymbolCountProvider;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TxWriter;
import io.questdb.std.FilesFacade;
import io.questdb.std.ObjList;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.cairo.TableUtils.COLUMN_VERSION_FILE_NAME;
import static io.questdb.cairo.TableUtils.TXN_FILE_NAME;

/**
 * Plan 3 (composite partitioning), Task 6: {@link TableReader}'s fresh-open path ({@code
 * initOpenPartitionInfo}) must carry each partition's {@code cellKey} in {@code openPartitionInfo}
 * (the previously-padding slot 6), and every {@code columnVersionReader.getMaxPartitionVersion(ts)}
 * call made while resolving a partition's column-version must pass that partition's own {@code
 * cellKey} -- otherwise two cells sharing a timestamp alias onto the same column-version, silently
 * corrupting one cell's resolved state with the other's.
 * <p>
 * This is the reader-side counterpart of Tasks 1-5 ({@code _txn}/{@code _cv} writer+reader storage).
 * Production never creates a multi-cell partition set in this plan (write-routing is Plan 4), so this
 * test synthesizes one directly through the {@code *ForTest} seams established by {@link
 * CompositeTxCellTest} and {@link CompositeColumnVersionCellTest}, mirroring real {@code
 * TableWriter}/{@code ColumnVersionWriter} commit sequencing -- commit {@code _cv} first, thread its
 * resulting version into the standalone {@code TxWriter} via {@code setColumnVersion} (exactly {@code
 * columnVersionWriter.commit(); txWriter.setColumnVersion(columnVersionWriter.getVersion());} in real
 * {@code TableWriter} code), then commit {@code _txn} -- so a genuinely fresh {@link TableReader}'s
 * {@code reloadColumnVersion()} converges instead of spinning to a timeout.
 * <p>
 * Deliberately does NOT touch {@code reconcileOpenPartitions}/{@code reconcileOpenPartitions0} (the
 * reload-diff path) -- that is Task 7. This test only exercises the initial, straight per-physical-index
 * load ({@code initOpenPartitionInfo}), never a reload of an already-open reader.
 */
public class CompositeReaderCellTest extends AbstractCairoTest {

    @Test
    public void testOpenPartitionInfoCarriesCellKeyAndResolvesColumnVersionPerCell() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table c (ts timestamp, exchange symbol, x double) " +
                    "timestamp(ts) partition by day, exchange");
            engine.releaseInactive(); // no pooled writer/reader may keep _txn/_cv open under our direct use

            final long day1 = 0L;
            final int col = 3;

            FilesFacade ff = engine.getConfiguration().getFilesFacade();
            TableToken tableToken = engine.verifyTableName("c");

            // 1. Raw _cv surgery: two cells at day1, distinct column-versions (100 vs 200) -- this is
            // exactly the signal TableReader's PARTITIONS_SLOT_OFFSET_COLUMN_VERSION slot is populated
            // from (via columnVersionReader.getMaxPartitionVersion), so it discriminates aliasing.
            long cvVersion;
            try (
                    Path path = new Path();
                    ColumnVersionWriter cvWriter = new ColumnVersionWriter(
                            configuration,
                            path.of(configuration.getDbRoot()).concat(tableToken).concat(COLUMN_VERSION_FILE_NAME).$(),
                            true
                    )
            ) {
                cvWriter.upsert(day1, 0, col, 100, 0);
                cvWriter.upsert(day1, 1, col, 200, 0);
                cvWriter.commit();
                cvVersion = cvWriter.getVersion();
            }

            // 2. Raw _txn surgery: two partitions at day1 -- cell0 and cell1 -- via the tail-append seam
            // (already in (ts, cellKey) order; distinct partitionNameTxn values 900/901, unrelated to the
            // _cv-derived column-version being tested). Threads the _cv version above into the TxWriter.
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(tableToken).concat(TXN_FILE_NAME).$();

                try (TxWriter txWriter = new TxWriter(ff, configuration)) {
                    txWriter.setCompositeForTest(true);
                    txWriter.ofRW(path.$(), ColumnType.TIMESTAMP_MICRO, PartitionBy.DAY);

                    // ofRW()'s unsafeLoadAll() has already loaded the real, current per-symbol value
                    // counts (the "exchange" column, plus this composite table's dedicated-dict/registry
                    // interner slots -- see CompositeInternerLayout). commit() rewrites this whole region
                    // from whatever SymbolCountProvider list it's handed; passing an empty list here would
                    // zero the symbol map writer count and break a subsequent real TableReader's
                    // openSymbolMaps(), which indexes into it. Pass through the real counts unchanged.
                    final int symbolColumnCount = txWriter.getSymbolColumnCount();
                    ObjList<SymbolCountProvider> symbolCountProviders = new ObjList<>();
                    for (int i = 0; i < symbolColumnCount; i++) {
                        final int count = txWriter.getSymbolValueCount(i);
                        symbolCountProviders.add(() -> count);
                    }

                    txWriter.appendPartitionForTest(day1, 5L, 900L, 0);
                    txWriter.appendPartitionForTest(day1, 5L, 901L, 1);
                    txWriter.updateMaxTimestamp(day1 + 1);
                    txWriter.finishPartitionSizeUpdate();
                    txWriter.setColumnVersion(cvVersion);
                    txWriter.commit(symbolCountProviders);
                }
            }

            // 3. Open a genuinely FRESH TableReader (never seen this _txn/_cv content before) and assert
            // its openPartitionInfo carries the real per-partition cellKey, and resolves each partition's
            // own column-version -- no aliasing across the two cells sharing day1.
            try (TableReader reader = getReader("c")) {
                Assert.assertEquals(2, reader.getPartitionCount());

                Assert.assertEquals("physical index 0 must be cell0", 0, reader.getPartitionCellKey(0));
                Assert.assertEquals("physical index 1 must be cell1", 1, reader.getPartitionCellKey(1));

                Assert.assertEquals(
                        "cell0's column-version must be its own (100), not aliased",
                        100, reader.getPartitionColumnVersion(0)
                );
                Assert.assertEquals(
                        "cell1's column-version must be its own (200), not aliased onto cell0's",
                        200, reader.getPartitionColumnVersion(1)
                );
            }
        });
    }
}
