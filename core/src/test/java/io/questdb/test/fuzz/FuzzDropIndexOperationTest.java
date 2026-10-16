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
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

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

    private static void assertIndexType(byte expectedIndexType) {
        try (TableMetadata metadata = engine.getTableMetadata(engine.verifyTableName("t"))) {
            Assert.assertEquals(expectedIndexType, metadata.getColumnIndexType(metadata.getColumnIndex("s")));
        }
    }
}
