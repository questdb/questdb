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

package io.questdb.test.cairo.wal;

import io.questdb.PropertyKey;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.SnapshotMarker;
import io.questdb.cairo.TableReaderMetadata;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TxReader;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * A durable epoch must never be published from a writer that is part-way through a structural change.
 * <p>
 * The epoch's {@code _meta.epoch.N} and {@code _txn.epoch.N} are two independent copies that recovery
 * restores TOGETHER, so they must describe the same table shape. {@code _txn}'s symbol area is written from
 * {@code denseSymbolMapWriters}; {@code _meta} carries the live symbol columns. Those counts agreeing is
 * exactly the precondition for a mutually consistent pair.
 * <p>
 * {@code TableWriter.changeColumnType} transiently breaks it: it creates the destination SYMBOL column's map
 * writer BEFORE running the conversion, and publishes the column to {@code _meta} only afterwards. When the
 * conversion has to pull a partition back from parquet, it takes a durable epoch cut from inside that window
 * ({@code ConvertOperatorImpl.convertColumn -> TableWriter.commitPendingParquetToNativeConversions ->
 * advanceDurableEpoch}). The resulting epoch records a {@code _txn} that counts the not-yet-published symbol
 * column against a {@code _meta} that lacks it — at the SAME {@code metadataVersion}, so every identity
 * check in {@code RecoveryCoordinator.epochCopiesValid} passes and the pair is adopted.
 * <p>
 * Adopting it rewinds the table into a state nothing can open: {@code rollbackSymbolTables} iterates the
 * {@code _txn} count over the shorter {@code denseSymbolMapWriters} and throws ArrayIndexOutOfBounds inside
 * the {@code TableWriter} constructor; past that, WAL apply rejects the table with "unexpected new WAL
 * structure version". The table stays permanently suspended while the sequencer runs ahead. It surfaced as
 * an {@code ArrayIndexOutOfBoundsException} in the adaptive crash-fuzz soak, thousands of crash points from
 * anything that looked structural.
 * <p>
 * This test asserts the invariant directly on the published epoch, with no crash machinery: whatever the
 * cadence decides to publish, the two copies must agree. Declining to publish is a valid outcome (the method
 * is best-effort by contract), which is why the assertion is conditional on an epoch existing.
 */
public class AdaptiveEpochStructuralConsistencyTest extends AbstractCairoTest {

    @Test
    public void testEpochCopiesAgreeOnSymbolColumnsAcrossConversionToSymbol() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (ts TIMESTAMP, sym SYMBOL, v VARCHAR) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO x SELECT timestamp_sequence('2024-01-01T00:00:00.000000Z', 60_000_000L), " +
                    "'s_' || (x % 5), 'value_long_string_' || x FROM long_sequence(200)");
            // A second partition so 2024-01-01 is not the active one when it is converted.
            execute("INSERT INTO x SELECT timestamp_sequence('2024-01-02T00:00:00.000000Z', 60_000_000L), " +
                    "'s2_' || (x % 5), 'value_long_string_d2_' || x FROM long_sequence(50)");
            drainWalQueue();

            // Parquet storage is what forces the conversion to pull the partition back to native, which is
            // the path that takes an epoch cut from inside the structural window.
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();

            // The trigger: a non-symbol column becomes a SYMBOL, so a NEW symbol map writer exists before
            // _meta names the column.
            execute("ALTER TABLE x ALTER COLUMN v TYPE SYMBOL");
            drainWalQueue();

            final TableToken token = engine.verifyTableName("x");
            Assert.assertFalse(
                    "table was suspended by the conversion — recovery adopted an inconsistent epoch",
                    engine.getTableSequencerAPI().isSuspended(token)
            );
            assertEpochCopiesAgreeOnSymbolCount(token);
        });
    }

    /**
     * Reads the epoch copies the way {@link io.questdb.cairo.RecoveryCoordinator} does and asserts the one
     * property recovery depends on but never checked: that the {@code _txn} and {@code _meta} it restores
     * together were captured from the same table shape.
     */
    private void assertEpochCopiesAgreeOnSymbolCount(TableToken token) {
        try (Path path = new Path()) {
            path.of(engine.getConfiguration().getDbRoot()).concat(token).concat(TableUtils.SNAPSHOT_FILE_NAME);
            final SnapshotMarker marker = new SnapshotMarker(engine.getConfiguration());
            final int generation;
            try {
                marker.of(path.$());
                if (!marker.tryLoad() || marker.getGeneration() == SnapshotMarker.LEGACY_GENERATION) {
                    // No metadata-bound epoch was published. Legitimate: the cut is best-effort, and
                    // declining to publish is exactly what the fix does in the structural window.
                    return;
                }
                generation = marker.getGeneration();
            } finally {
                marker.close();
            }

            final int rootLen = path.of(engine.getConfiguration().getDbRoot()).concat(token).size();

            path.trimTo(rootLen).concat(TableUtils.META_FILE_NAME)
                    .put(TableUtils.EPOCH_COPY_SUFFIX).put('.').put(generation);
            int metaSymbolColumns = 0;
            int partitionBy;
            int timestampType;
            try (TableReaderMetadata epochMetadata = new TableReaderMetadata(engine.getConfiguration())) {
                epochMetadata.loadMetadata(path.$());
                partitionBy = epochMetadata.getPartitionBy();
                timestampType = epochMetadata.getTimestampType();
                for (int i = 0, n = epochMetadata.getColumnCount(); i < n; i++) {
                    final int type = epochMetadata.getColumnType(i);
                    if (type > -1 && ColumnType.isSymbol(type)) {
                        metaSymbolColumns++;
                    }
                }
            }

            path.trimTo(rootLen).concat(TableUtils.TXN_FILE_NAME)
                    .put(TableUtils.EPOCH_COPY_SUFFIX).put('.').put(generation);
            final int txnSymbolColumns;
            try (TxReader txReader = new TxReader(engine.getConfiguration().getFilesFacade())) {
                txReader.ofRO(path.$(), timestampType, partitionBy);
                Assert.assertTrue("could not load _txn.epoch." + generation, txReader.unsafeLoadAll());
                txnSymbolColumns = txReader.getSymbolColumnCount();
            }

            Assert.assertEquals(
                    "the durable epoch's _txn and _meta copies disagree on symbol column count, so recovery "
                            + "would restore a _txn/_meta pair that never coexisted. This is published when an "
                            + "epoch cut is taken from inside changeColumnType()'s structural window — the "
                            + "symbol map writer exists but _meta does not yet name the column. Recovery then "
                            + "rolls the table back into a state nothing can open (ArrayIndexOutOfBounds in "
                            + "rollbackSymbolTables, then permanent suspension). advanceDurableEpoch() must "
                            + "decline to publish while that window is open.",
                    metaSymbolColumns,
                    txnSymbolColumns
            );
        }
    }
}
