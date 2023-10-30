/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package io.questdb.test.griffin;

import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.std.*;
import io.questdb.std.str.Path;
import io.questdb.test.fuzz.FuzzTransaction;
import io.questdb.test.griffin.wal.AbstractFuzzTest;
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
                0.1 * rnd.nextDouble(),
                0.01,
                0.0
        );

        partitionCount = 5 + rnd.nextInt(10);
        setFuzzCounts(
                rnd.nextBoolean(),
                rnd.nextInt(2_000_000),
                rnd.nextInt(1000),
                rnd.nextInt(1000),
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
                1,
                0.1 * rnd.nextDouble(),
                0.01,
                0.0
        );


        partitionCount = 5 + rnd.nextInt(10);
        setFuzzCounts(
                rnd.nextBoolean(),
                20_000,
                20,
                rnd.nextInt(1000),
                rnd.nextInt(1000),
                rnd.nextInt(1000),
                rnd.nextInt(1_000_000),
                partitionCount
        );

        runFuzz(rnd);
    }

    private void copyTableDir(TableToken src, TableToken merged) {
        FilesFacade ff = configuration.getFilesFacade();

        Path pathDest = Path.getThreadLocal(configuration.getRoot()).concat(merged).$();
        Path pathSrc = Path.getThreadLocal2(configuration.getRoot()).concat(src).$();

        ff.rmdir(pathDest);
        ff.copyRecursive(pathSrc, pathDest, configuration.getMkDirMode());
    }

    private void mergeAllPartitions(TableToken merged) {
        FilesFacade ff = configuration.getFilesFacade();
        try (TableWriter writer = getWriter(merged)) {
            writer.squashAllPartitionsIntoOne();
        }

        engine.releaseInactive();

        // Force overwrite partitioning to by YEAR
        Path path = Path.getThreadLocal(configuration.getRoot()).concat(merged).concat(TableUtils.META_FILE_NAME).$();
        int metaFd = TableUtils.openRW(ff, path, LOG, configuration.getWriterFileOpenOpts());

        long addr = Unsafe.malloc(4, MemoryTag.NATIVE_DEFAULT);
        Unsafe.getUnsafe().putInt(addr, PartitionBy.YEAR);
        ff.write(metaFd, addr, 4, TableUtils.META_OFFSET_PARTITION_BY);

        Unsafe.getUnsafe().putInt(addr, merged.getTableId());
        ff.write(metaFd, addr, 4, TableUtils.META_OFFSET_TABLE_ID);

        Unsafe.free(addr, 4, MemoryTag.NATIVE_DEFAULT);
        ff.close(metaFd);
    }

    @Override
    protected void runFuzz(Rnd rnd) throws Exception {
        configOverrideO3ColumnMemorySize(rnd.nextInt(16 * 1024 * 1024));

        String tableName = testName.getMethodName();
        String tableNameMerged = testName.getMethodName() + "_merged";
        TableToken merged = fuzzer.createInitialTable(tableNameMerged, false, 0);

        assertMemoryLeak(() -> {
            TableToken src = fuzzer.createInitialTable(tableName, false);
            ObjList<FuzzTransaction> transactions = fuzzer.generateTransactions(tableName, rnd);

            fuzzer.applyNonWal(transactions, tableName, rnd);
            engine.releaseInactive();

            copyTableDir(src, merged);
            mergeAllPartitions(merged);
        });

        String limit = "";
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                tableName + limit,
                tableNameMerged + limit,
                LOG
        );
        fuzzer.assertRandomIndexes(tableName, tableNameMerged, rnd);
    }

}
