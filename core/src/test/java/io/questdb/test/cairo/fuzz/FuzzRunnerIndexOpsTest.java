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

import io.questdb.cairo.IndexType;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.fuzz.FuzzAddCoveringIndexOperation;
import io.questdb.test.fuzz.FuzzDropIndexOperation;
import io.questdb.test.fuzz.FuzzTransaction;
import org.junit.Assert;
import org.junit.Test;

public class FuzzRunnerIndexOpsTest extends AbstractCairoTest {

    @Test
    public void testWalBatchDropIndexOfColumnCoveredInSameBatch() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (c2 SYMBOL, ts TIMESTAMP, sym2 SYMBOL INDEX TYPE BITMAP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO t SELECT
                        CASE WHEN x % 3 = 0 THEN NULL ELSE 'k' || (x % 5) END,
                        dateadd('m', x::INT * 7, '2022-02-26T00:00:00Z'::TIMESTAMP),
                        'S' || (x % 11)
                    FROM long_sequence(400)
                    """);
            drainWalQueue();

            // one batch: c2 gets a POSTING index that covers sym2, then the index of sym2 is to change
            // from BITMAP to POSTING; the batch queues the cover before the drop op reads the metadata
            final IntList include = new IntList();
            include.add(2);
            final FuzzTransaction coverTransaction = new FuzzTransaction();
            coverTransaction.operationList.add(new FuzzAddCoveringIndexOperation(0, include, "POSTING"));
            coverTransaction.rollback = true;
            final FuzzTransaction dropTransaction = new FuzzTransaction();
            dropTransaction.operationList.add(new FuzzDropIndexOperation(2, "POSTING"));
            dropTransaction.rollback = true;
            final ObjList<FuzzTransaction> transactions = new ObjList<>();
            transactions.add(coverTransaction);
            transactions.add(dropTransaction);

            final FuzzRunner fuzzer = new FuzzRunner();
            fuzzer.withDb(engine, sqlExecutionContext);
            fuzzer.applyWal(transactions, "t", 1, new Rnd());

            // the drop op sees the cover and leaves sym2 as it is
            try (TableMetadata metadata = engine.getTableMetadata(engine.verifyTableName("t"))) {
                Assert.assertEquals(IndexType.POSTING, metadata.getColumnIndexType(0));
                Assert.assertEquals("[2,1]", String.valueOf(metadata.getColumnMetadata(0).getCoveringColumnIndices()));
                Assert.assertEquals(IndexType.BITMAP, metadata.getColumnIndexType(2));
            }
            assertQuery("SELECT c2, sym2, ts FROM t WHERE c2 = NULL LIMIT 3")
                    .noLeakCheck()
                    .timestamp("ts")
                    .noRandomAccess()
                    .expectSize()
                    .withPlanContaining("CoveringIndex backup: true on: c2 with: sym2, ts")
                    .returns("""
                            c2\tsym2\tts
                            \tS3\t2022-02-26T00:21:00.000000Z
                            \tS6\t2022-02-26T00:42:00.000000Z
                            \tS9\t2022-02-26T01:03:00.000000Z
                            """);
            assertSqlCursors(
                    "SELECT /*+ no_covering */ c2, sym2, ts FROM t WHERE c2 = NULL",
                    "SELECT c2, sym2, ts FROM t WHERE c2 = NULL"
            );
        });
    }
}
