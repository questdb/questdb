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

package io.questdb.test.cairo.parquet;

import io.questdb.PropertyKey;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.idx.IndexReader;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.cairo.vm.api.MemoryCR;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Native partition with a symbol index -> CONVERT TO PARQUET -> in-place O3 update -> CONVERT TO
 * NATIVE. The rebuilt native partition's index must agree with its symbol column.
 */
public class ParquetRoundTripIndexTest extends AbstractCairoTest {
    private static final long DAY = 86_400_000_000L;

    @Test
    public void testIndexAfterParquetRoundTripWithInPlaceUpdate() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 64);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, s SYMBOL INDEX, x LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            final TableToken token = engine.verifyTableName("t");
            try (TableWriter w = engine.getWriter(token, "test")) {
                for (int i = 0; i < 400; i++) {
                    putRow(w, i * 100_000_000L, i);
                }
                for (int i = 0; i < 10; i++) {
                    putRow(w, DAY + i * 1_000_000L, i);
                }
                w.commit();
                verify(token, "native before conversion");

                Assert.assertTrue(w.convertPartitionNativeToParquet(0, null, 0.01));
                verify(token, "parquet after conversion");

                // O3 rows inside day 0 that interleave with existing row groups: in-place update
                for (int i = 0; i < 50; i++) {
                    putRow(w, 350 * 100_000_000L + 50_000_000L + i * 1_000_000L, 1000 + i);
                }
                w.commit();
                try (TableReader r = engine.getReader(token)) {
                    Assert.assertTrue("day 0 must still be parquet, i.e. updated in place", r.getTxFile().isPartitionParquet(0));
                }
                verify(token, "parquet after in-place update");

                Assert.assertTrue(w.convertPartitionParquetToNative(0));
                verify(token, "native after round trip");
            }
        });
    }

    private static void verify(TableToken token, String label) {
        try (TableReader reader = engine.getReader(token)) {
            final int p = 0;
            final long rows = reader.openPartition(p);
            if (reader.getTxFile().isPartitionParquet(p)) {
                return; // parquet columns are not mapped as native memory; checked after conversion back
            }
            final int symCol = reader.getMetadata().getColumnIndex("s");
            final MemoryCR symMem = reader.getColumn(TableReader.getPrimaryColumnIndex(reader.getColumnBase(p), symCol));
            for (int dir = 1; dir <= 2; dir++) {
                final IndexReader ir = reader.getIndexReader(p, symCol, dir);
                for (int key = 0; key < 7; key++) {
                    long colCount = 0;
                    for (long r = 0; r < rows; r++) {
                        if (symMem.getInt(r << 2) == key) colCount++;
                    }
                    final RowCursor c = ir.getCursor(TableUtils.toIndexKey(key), 0, rows - 1);
                    long idxCount = 0;
                    while (c.hasNext()) {
                        final long row = c.next();
                        idxCount++;
                        Assert.assertTrue(label + ": row out of range " + row, row >= 0 && row < rows);
                        Assert.assertEquals(label + ": index key " + key + " at row " + row + " but column holds " + symMem.getInt(row << 2) + " (dir " + dir + ")", key, symMem.getInt(row << 2));
                    }
                    Assert.assertEquals(label + ": rows for key " + key + " (dir " + dir + ")", colCount, idxCount);
                }
            }
        }
    }

    private static void putRow(TableWriter w, long ts, long x) {
        TableWriter.Row row = w.newRow(ts);
        row.putSym(1, "S" + (x % 7));
        row.putLong(2, x);
        row.append();
    }
}
