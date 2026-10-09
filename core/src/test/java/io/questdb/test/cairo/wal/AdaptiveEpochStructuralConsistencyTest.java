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
import io.questdb.cairo.DurableEpochManifest;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableReaderMetadata;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TxReader;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * A durable epoch must never be published from a writer that is part-way through a structural change.
 * <p>
 * Recovery restores the save point's copies of {@code _meta} and {@code _txn} together, so they must describe
 * the same table shape: the symbol columns in {@code _meta} must match the symbol area the writer writes to
 * {@code _txn} from {@code denseSymbolMapWriters}. {@code TableWriter.changeColumnType} to SYMBOL creates the
 * new map writer before it publishes the column to {@code _meta}. An epoch cut in that window would pass the
 * identity checks in {@code RecoveryCoordinator.epochCopiesValid}, since the {@code metadataVersion} matches,
 * and restore a table that cannot open: WAL apply rejects it and the table stays suspended.
 * <p>
 * The test converts a column of a parquet partition to SYMBOL and checks the published save point directly,
 * with no crash machinery. The cut is best-effort, so publishing nothing is valid, and the assertion applies
 * only when an epoch exists.
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

            // Parquet storage makes the conversion pull the partition back to native inside the structural
            // window.
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();

            // The trigger: a non-symbol column becomes a SYMBOL, so a new symbol map writer exists before
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
     * Reads the saved sections the way {@link io.questdb.cairo.RecoveryCoordinator} does and asserts a
     * property recovery depends on but does not check: the {@code _txn} and {@code _meta} it restores together
     * come from the same table shape.
     */
    private void assertEpochCopiesAgreeOnSymbolCount(TableToken token) {
        try (Path path = new Path()) {
            path.of(engine.getConfiguration().getDbRoot()).concat(token);
            final DurableEpochManifest.Candidate[] saved = DurableEpochManifest.loadCandidates(engine.getConfiguration().getFilesFacade(), path, path.size(), token.getTableId());
            if (saved.length == 0) {
                // No epoch was published: the cut is best-effort, and declines to publish in the structural
                // window.
                return;
            }
            final int generation = saved[0].generation;

            final FilesFacade ff = engine.getConfiguration().getFilesFacade();
            final CharSequence dbRoot = engine.getConfiguration().getDbRoot();
            // The readers load files, so each saved section is copied out first.
            path.of(dbRoot).concat("saved" + TableUtils.META_FILE_NAME);
            TestUtils.copySavePointSection(ff, dbRoot, token, generation, TableUtils.META_FILE_NAME, path.$());
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

            path.of(dbRoot).concat("saved" + TableUtils.TXN_FILE_NAME);
            TestUtils.copySavePointSection(ff, dbRoot, token, generation, TableUtils.TXN_FILE_NAME, path.$());
            final int txnSymbolColumns;
            try (TxReader txReader = new TxReader(ff)) {
                txReader.ofRO(path.$(), timestampType, partitionBy);
                Assert.assertTrue("could not load the saved _txn of generation " + generation, txReader.unsafeLoadAll());
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
