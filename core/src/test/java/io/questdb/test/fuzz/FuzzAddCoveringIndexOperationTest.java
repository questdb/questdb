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

import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.std.IntList;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class FuzzAddCoveringIndexOperationTest extends AbstractCairoTest {

    @Test
    public void testApplyAddsCoveringIndexThroughTableWriter() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, s SYMBOL, v DOUBLE) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 'A', 1.0)");
            try (TableWriter writer = getWriter("t")) {
                newOperation("t", "v").apply(new Rnd(), engine, writer, -1, null);
            }
            assertCoveringIndex("t");
        });
    }

    @Test
    public void testApplyAddsCoveringIndexThroughWalWriter() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, s SYMBOL, v DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 'A', 1.0)");
            drainWalQueue();
            try (WalWriter writer = getWalWriter("t")) {
                newOperation("t", "v").apply(new Rnd(), engine, writer, -1, null);
            }
            drainWalQueue();
            assertCoveringIndex("t");
        });
    }

    @Test
    public void testApplySkipsSymbolColumnReplacedByNonSymbol() throws Exception {
        // The operation keeps the column index it was generated with. A drop and re-add under the same name, not yet
        // applied to the table, must leave it with nothing to index rather than an ALTER on a non-symbol column.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, s SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();
            final FuzzAddCoveringIndexOperation op = newOperation("t", "ts");
            execute("ALTER TABLE t DROP COLUMN s");
            execute("ALTER TABLE t ADD COLUMN s BYTE");
            try (WalWriter writer = getWalWriter("t")) {
                op.apply(new Rnd(), engine, writer, -1, null);
            }
            drainWalQueue();
            final TableToken tableToken = engine.verifyTableName("t");
            try (TableMetadata metadata = engine.getTableMetadata(tableToken)) {
                Assert.assertFalse(metadata.isColumnIndexed(metadata.getColumnIndex("s")));
            }
        });
    }

    private static void assertCoveringIndex(String tableName) {
        try (TableMetadata metadata = engine.getTableMetadata(engine.verifyTableName(tableName))) {
            final int symbolColumnIndex = metadata.getColumnIndex("s");
            Assert.assertTrue(metadata.isColumnIndexed(symbolColumnIndex));
            final IntList covered = metadata.getColumnMetadata(symbolColumnIndex).getCoveringColumnIndices();
            Assert.assertNotNull(covered);
            Assert.assertTrue(covered.contains(metadata.getWriterIndex(metadata.getColumnIndex("v"))));
        }
    }

    private static FuzzAddCoveringIndexOperation newOperation(String tableName, String includeColumnName) {
        try (TableMetadata metadata = engine.getTableMetadata(engine.verifyTableName(tableName))) {
            final IntList includeColumnIndices = new IntList();
            includeColumnIndices.add(metadata.getColumnIndex(includeColumnName));
            return new FuzzAddCoveringIndexOperation(metadata.getColumnIndex("s"), includeColumnIndices, "POSTING");
        }
    }
}
