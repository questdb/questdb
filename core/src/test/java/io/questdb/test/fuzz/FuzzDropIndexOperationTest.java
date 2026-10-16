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

package io.questdb.test.fuzz;

import io.questdb.cairo.IndexType;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.TreeSet;

public class FuzzDropIndexOperationTest extends AbstractCairoTest {

    @Test
    public void testExecutePostDrain() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, s SYMBOL INDEX TYPE POSTING, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO t VALUES ('2024-01-01T01:00:00.000000Z', 'a', 1), ('2024-01-02T01:00:00.000000Z', 'b', 2)");
            drainWalQueue();
            assertIndexType(IndexType.POSTING);

            // POSTING to BITMAP
            new FuzzDropIndexOperation(1, "BITMAP").executePostDrain(engine, "t");
            drainWalQueue();
            assertIndexType(IndexType.BITMAP);

            // BITMAP to no index
            new FuzzDropIndexOperation(1, null).executePostDrain(engine, "t");
            drainWalQueue();
            assertIndexType(IndexType.NONE);

            // a column without an index, a column that is not a symbol and a column out of range stay as
            // they are
            new FuzzDropIndexOperation(1, "POSTING").executePostDrain(engine, "t");
            new FuzzDropIndexOperation(2, "POSTING").executePostDrain(engine, "t");
            new FuzzDropIndexOperation(3, "POSTING").executePostDrain(engine, "t");
            drainWalQueue();
            assertIndexType(IndexType.NONE);
            assertQuery("SELECT s, x FROM t WHERE s = 'b'")
                    .noLeakCheck()
                    .returns("s\tx\nb\t2\n");
        });
    }

    @Test
    public void testGenerateSetEmitsDropIndexOperations() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, x LONG, s SYMBOL INDEX TYPE POSTING) TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();
            final TableToken tableToken = engine.verifyTableName("t");
            try (
                    TableRecordMetadata sequencerMetadata = engine.getLegacyMetadata(tableToken);
                    TableMetadata tableMetadata = engine.getTableMetadata(tableToken)
            ) {
                // DROP INDEX the only kind of transaction: each one drops the index of s, the only symbol
                // column, and indexes it again with one of the four index types or not at all
                final ObjList<FuzzTransaction> transactions = generateSet(sequencerMetadata, tableMetadata, 1, 0);
                Assert.assertEquals(100, transactions.size());
                final TreeSet<String> newIndexTypeNames = new TreeSet<>();
                for (int i = 0, n = transactions.size(); i < n; i++) {
                    final FuzzTransaction transaction = transactions.getQuick(i);
                    Assert.assertEquals(1, transaction.operationList.size());
                    Assert.assertTrue(transaction.rollback);
                    final FuzzDropIndexOperation operation = (FuzzDropIndexOperation) transaction.operationList.getQuick(0);
                    Assert.assertEquals(2, operation.getSymbolColumnIndex());
                    newIndexTypeNames.add(String.valueOf(operation.getNewIndexTypeName()));
                }
                Assert.assertEquals("[BITMAP, POSTING, POSTING DELTA, POSTING EF, null]", newIndexTypeNames.toString());

                // with no DROP INDEX probability, the generator draws the transactions it drew before
                // DROP INDEX existed: inserts only here
                final ObjList<FuzzTransaction> inserts = generateSet(sequencerMetadata, tableMetadata, 0, 1);
                Assert.assertTrue(inserts.size() > 0);
                for (int i = 0, n = inserts.size(); i < n; i++) {
                    final FuzzTransaction transaction = inserts.getQuick(i);
                    for (int j = 0, m = transaction.operationList.size(); j < m; j++) {
                        Assert.assertTrue(transaction.operationList.getQuick(j) instanceof FuzzInsertOperation);
                    }
                }
            }
        });
    }

    private static ObjList<FuzzTransaction> generateSet(
            TableRecordMetadata sequencerMetadata,
            TableMetadata tableMetadata,
            double probabilityOfDropIndex,
            double probabilityOfDataInsert
    ) {
        final long minTimestamp = 1_704_067_200_000_000L; // 2024-01-01
        return FuzzTransactionGenerator.generateSet(
                0,
                sequencerMetadata,
                tableMetadata,
                new Rnd(42, 4242),
                minTimestamp,
                minTimestamp + 5 * 86_400_000_000L,
                100,
                100,
                false,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                probabilityOfDataInsert,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                5,
                new String[]{"a", "b", "c"},
                (int) sequencerMetadata.getMetadataVersion(),
                0,
                0,
                probabilityOfDropIndex,
                0,
                false
        );
    }

    private static void assertIndexType(byte expectedIndexType) {
        try (TableMetadata metadata = engine.getTableMetadata(engine.verifyTableName("t"))) {
            Assert.assertEquals(expectedIndexType, metadata.getColumnIndexType(metadata.getColumnIndex("s")));
        }
    }
}
