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

package io.questdb.test.cairo.fuzz;

import io.questdb.PropertyKey;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableMetadataFileBlock;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.file.BlockFileReader;
import io.questdb.cairo.file.BlockFileWriter;
import io.questdb.std.FilesFacade;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.str.Path;
import io.questdb.test.fuzz.FuzzTransaction;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

public class FrameAppendFuzzTest extends AbstractFuzzTest {
    private int partitionCount;

    @Test
    public void testFullRandom() throws Exception {
        Rnd rnd = generateRandom(LOG);
        setFuzzProperties(rnd.nextLong(50), getRndO3PartitionSplit(rnd), getRndO3PartitionSplit(rnd));

        setFuzzProbabilities(
                0.5 * rnd.nextDouble(),
                rnd.nextDouble(),
                rnd.nextDouble(),
                0.5 * rnd.nextDouble(),
                rnd.nextDouble(),
                rnd.nextDouble(),
                rnd.nextDouble(),
                rnd.nextDouble(),
                rnd.nextDouble(),
                0.01,
                0.1,
                0.1 * rnd.nextDouble(),
                0.0,
                0.4,
                0.0,
                0
        );

        partitionCount = 5 + rnd.nextInt(10);
        setFuzzCounts(
                rnd.nextBoolean(),
                rnd.nextInt(1_000_000),
                rnd.nextInt(500),
                fuzzer.randomiseStringLengths(rnd, 1000),
                rnd.nextInt(1000),
                rnd.nextInt(1000),
                rnd.nextInt(1_000_000),
                partitionCount
        );

        runFuzz(rnd);
    }

    @Test
    public void testSimple() throws Exception {
        Rnd rnd = generateRandom(LOG);
        setFuzzProperties(rnd.nextLong(50), getRndO3PartitionSplit(rnd), getRndO3PartitionSplit(rnd));

        setFuzzProbabilities(
                0,
                rnd.nextDouble(),
                rnd.nextDouble(),
                0,
                0.5,
                0,
                0,
                0.0,
                1,
                0.01,
                0.1,
                0.1 * rnd.nextDouble(),
                0.0,
                0.4,
                0.0,
                0
        );

        partitionCount = 5 + rnd.nextInt(10);
        setFuzzCounts(
                rnd.nextBoolean(),
                20_000,
                20,
                fuzzer.randomiseStringLengths(rnd, 1000),
                rnd.nextInt(1000),
                rnd.nextInt(1000),
                rnd.nextInt(500_000),
                partitionCount
        );

        runFuzz(rnd);
    }

    private void copyTableDir(TableToken src, TableToken merged) {
        FilesFacade ff = configuration.getFilesFacade();

        Path pathDest = Path.getThreadLocal(configuration.getDbRoot()).concat(merged);
        Path pathSrc = Path.getThreadLocal2(configuration.getDbRoot()).concat(src);

        ff.rmdir(pathDest);
        ff.copyRecursive(pathSrc, pathDest, configuration.getMkDirMode());
    }

    private void mergeAllPartitions(TableToken merged) {
        FilesFacade ff = configuration.getFilesFacade();
        engine.releaseInactive();

        // Force overwrite partitioning to by YEAR using BlockFile format
        Path path = Path.getThreadLocal(configuration.getDbRoot()).concat(merged).concat(TableUtils.META_FILE_NAME);

        // Read current metadata
        TableMetadataFileBlock.MetadataHolder holder = new TableMetadataFileBlock.MetadataHolder();
        try (BlockFileReader reader = new BlockFileReader(configuration)) {
            reader.of(path.$());
            TableMetadataFileBlock.read(reader, holder, path.$());
        }

        // Update partition type and table ID
        holder.partitionBy = PartitionBy.YEAR;
        holder.tableId = merged.getTableId();

        // Write back using BlockFile format
        try (BlockFileWriter writer = new BlockFileWriter(ff, configuration.getCommitMode())) {
            writer.of(path.$());
            TableMetadataFileBlock.write(
                    writer,
                    ColumnType.VERSION,
                    holder.tableId,
                    holder.partitionBy,
                    holder.timestampIndex,
                    holder.walEnabled,
                    holder.maxUncommittedRows,
                    holder.o3MaxLag,
                    holder.ttlHoursOrMonths,
                    holder.columns
            );
        }

        try (TableWriter writer = getWriter(merged)) {
            writer.squashAllPartitionsIntoOne();
        }
    }

    @Override
    protected void runFuzz(Rnd rnd) throws Exception {
        int size = rnd.nextInt(16 * 1024 * 1024);
        node1.setProperty(PropertyKey.DEBUG_CAIRO_O3_COLUMN_MEMORY_SIZE, size);

        String tableName = testName.getMethodName();
        String tableNameMerged = testName.getMethodName() + "_merged";
        TableToken merged = fuzzer.createInitialTableEmptyNonWal(tableNameMerged);

        assertMemoryLeak(() -> {
            TableToken src = fuzzer.createInitialTableNonWal(tableName, null);
            ObjList<FuzzTransaction> transactions = fuzzer.generateTransactions(tableName, rnd);
            try {
                fuzzer.applyNonWal(transactions, tableName, rnd);
                engine.releaseInactive();

                copyTableDir(src, merged);
                mergeAllPartitions(merged);

                String limit = ""; // For debugging
                TestUtils.assertSqlCursors(
                        engine,
                        sqlExecutionContext,
                        tableName + limit,
                        tableNameMerged + limit,
                        LOG
                );
                fuzzer.assertRandomIndexes(tableName, tableNameMerged, rnd);
            } finally {
                Misc.freeObjListAndClear(transactions);
            }
        });
    }
}
