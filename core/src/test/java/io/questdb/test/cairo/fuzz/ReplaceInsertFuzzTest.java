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

package io.questdb.test.cairo.fuzz;

import io.questdb.PropertyKey;
import io.questdb.cairo.ColumnType;
import io.questdb.std.Rnd;
import org.junit.Assert;
import org.junit.Test;

public class ReplaceInsertFuzzTest extends AbstractFuzzTest {
    // Regression for the POSTING column rename bug where
    // linkPostingIndexAuxFiles skipped the .pv link when readSealTxnFromKeyFile
    // returned -1, including the legitimate empty-chain (pre-seal) case. With
    // this seed the WAL apply path adds sym_top SYMBOL INDEX and immediately
    // renames it before any seal has run, so the live unsealed values still
    // live in .pv.<colTxn>.0. Without the fix, the renamed column ends up
    // with .d + .pk linked but no .pv, the table gets suspended on apply,
    // and the parallel WAL harness fails checkNoSuspendedTables.
    @Test
    public void testRenamePostingColumnBeforeFirstSeal() throws Exception {
        Rnd rnd = generateRandom(LOG, 7267207179444742L, 1778696418556L);
        setFuzzProbabilities(
                0.01,
                0.2,
                0.1,
                0.01,
                0.15,
                0.05,
                0.08,
                0.15,
                1.0,
                0.01,
                0.1,
                0.01,
                0.01,
                0.8,
                0.05,
                0.05
        );
        setFuzzCounts(
                rnd.nextBoolean(), 1000, 5 + rnd.nextInt(100),
                20, 10, 200, rnd.nextInt(1000), 1
        );
        runFuzz(rnd);
    }

    @Test
    public void testReplaceOnFormatParquetTable() throws Exception {
        runReplaceOnFormatParquetTable(ColumnType.TIMESTAMP_MICRO);
    }

    @Test
    public void testReplaceOnFormatParquetTableNanos() throws Exception {
        runReplaceOnFormatParquetTable(ColumnType.TIMESTAMP_NANO);
    }

    @Test
    public void testReplaceWithParquetConversions() throws Exception {
        runReplaceWithParquetConversions(ColumnType.TIMESTAMP_MICRO);
    }

    @Test
    public void testReplaceWithParquetConversionsNanos() throws Exception {
        runReplaceWithParquetConversions(ColumnType.TIMESTAMP_NANO);
    }

    @Test
    public void testSimpleDataTransactionBigReplaceProb() throws Exception {
        Rnd rnd = generateRandom(LOG);
        setFuzzProbabilities(
                0.01,
                0.2,
                0.1,
                0.01,
                0.02,
                0.02,
                0.08,
                0,
                1.0,
                0.01,
                0.1,
                0.01,
                0.01,
                0.8,
                0.9,
                0.05
        );
        setFuzzCounts(
                rnd.nextBoolean(), 1000, 5 + rnd.nextInt(100),
                20, 10, 200, rnd.nextInt(100), 1
        );
        runFuzz(rnd);
    }

    @Test
    public void testSimpleDataTransactionSmallReplaceProb() throws Exception {
        Rnd rnd = generateRandom(LOG);
        setFuzzProbabilities(
                0.01,
                0.2,
                0.1,
                0.01,
                0.15,
                0.05,
                0.08,
                0.15,
                1.0,
                0.01,
                0.1,
                0.01,
                0.01,
                0.8,
                0.05,
                0.05
        );
        setFuzzCounts(
                rnd.nextBoolean(), 1000, 5 + rnd.nextInt(100),
                20, 10, 200, rnd.nextInt(1000), 1
        );
        runFuzz(rnd);
    }

    private void runReplaceOnFormatParquetTable(int timestampType) throws Exception {
        Rnd rnd = generateRandom(LOG);
        fuzzer.setTimestampType(timestampType);
        setCreateWalAsParquet(true);
        setFuzzProbabilities(
                0.01, 0.2, 0.1, 0.01,
                0.02, 0.02, 0.02, 0,
                1.0, 0.01, 0.01,
                0, 0, 0, 0, 0,
                0.5, 0.05, 0
        );
        setReplaceOnParquetCounts(rnd);
        setSmallParquetRowGroups(rnd);
        final O3ParquetPathCounts counts = new O3ParquetPathCounts();
        runFuzz(rnd);
        counts.log(LOG, getTestName());
        // every partition is parquet, so every data commit writes one
        Assert.assertTrue("no parquet O3 ran: " + counts, counts.getWritten() > 0);
    }

    private void runReplaceWithParquetConversions(int timestampType) throws Exception {
        Rnd rnd = generateRandom(LOG);
        fuzzer.setTimestampType(timestampType);
        setFuzzProbabilities(
                0.01, 0.2, 0.1, 0.01,
                0.02, 0.02, 0.02, 0,
                1.0, 0.01, 0.01,
                0.2, 0.02, 0, 0, 0,
                0.5, 0.05, 0
        );
        setReplaceOnParquetCounts(rnd);
        setSmallParquetRowGroups(rnd);
        final O3ParquetPathCounts counts = new O3ParquetPathCounts();
        runFuzz(rnd);
        counts.log(LOG, getTestName());
    }

    // A few thousand rows over 2 to 4 partitions and up to 200 transactions, half of them
    // replace commits: cheap, and enough commits per partition to reach in-place merges,
    // in-place DROPs, no-op windows and partition removals.
    private void setReplaceOnParquetCounts(Rnd rnd) {
        setFuzzCounts(
                rnd.nextBoolean(), 5_000, 50 + rnd.nextInt(150),
                20, 10, 200, rnd.nextInt(1000), 2 + rnd.nextInt(3)
        );
    }

    // Fuzz partitions hold ~1_000 rows, far below the default parquet row group size, so
    // each partition would be a single row group: every replace would rewrite it, and a
    // row group fully covered by a replace window (DROP) could not occur. A few rows per
    // row group spreads replace windows across row groups and exercises in-place merges.
    private void setSmallParquetRowGroups(Rnd rnd) {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 5 + rnd.nextInt(30));
        setRndParquetRewriteGate(rnd);
    }
}
