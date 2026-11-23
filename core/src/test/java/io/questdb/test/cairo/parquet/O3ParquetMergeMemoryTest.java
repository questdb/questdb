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

package io.questdb.test.cairo.parquet;

import io.questdb.PropertyKey;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * An O3 merge into a parquet partition decodes row groups into buffers that the worker reuses. A
 * deduplicating commit at a timestamp that a run of row groups shares decodes the whole run at once,
 * so the worker must not keep the buffers after the merge: it would hold the memory of its largest
 * decode for good.
 */
public class O3ParquetMergeMemoryTest extends AbstractCairoTest {

    @Override
    @Before
    public void setUp() {
        super.setUp();
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 1000);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_RATIO, "1.0");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_MAX_BYTES, Long.MAX_VALUE);
    }

    @Test
    public void testDedupTieRunMergeFreesDecodeBuffers() throws Exception {
        // 10,000 rows at 12:00 make a run of 10 row groups that share the timestamp, the commit at 12:00
        // merges the whole run
        assertMemoryLeak(() -> {
            createParquetTable("DEDUP UPSERT KEYS(ts, id)");
            assertCommitFreesDecodeBuffers("INSERT INTO t VALUES ('2024-01-01T12:00:00.000000Z', 1_000_000, 'new', 1.0)", 10_004);
            assertCommitFreesDecodeBuffers("INSERT INTO t VALUES ('2024-01-01T12:00:00.000000Z', 7, 'upd', 2.0)", 10_004);
            assertQuery("SELECT s, d FROM t WHERE id = 7").noLeakCheck().returns("s\td\nupd\t2.0\n");
        });
    }

    @Test
    public void testInPlaceMergeFreesDecodeBuffers() throws Exception {
        assertMemoryLeak(() -> {
            createParquetTable("");
            assertCommitFreesDecodeBuffers("INSERT INTO t VALUES ('2024-01-01T03:00:00.000000Z', 1_000_000, 'new', 1.0)", 10_004);
        });
    }

    private void assertCommitFreesDecodeBuffers(String insert, long expectedRowCount) throws Exception {
        final long before = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_PARQUET_PARTITION_UPDATER);
        execute(insert);
        drainWalQueue();
        assertQuery("SELECT count() FROM t").noLeakCheck().noRandomAccess().expectSize().returns("count\n" + expectedRowCount + "\n");
        Assert.assertEquals(before, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_PARQUET_PARTITION_UPDATER));
    }

    private static void createParquetTable(String dedup) throws Exception {
        execute("CREATE TABLE t (ts TIMESTAMP, id LONG, s VARCHAR, d DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL " + dedup);
        // 10,000 rows at 12:00 between 1 row at 00:00 and 1 row at 23:00
        execute("""
                INSERT INTO t
                SELECT '2024-01-01T12:00:00.000000Z'::TIMESTAMP, x, 'v' || x, x::DOUBLE FROM long_sequence(10_000)
                UNION ALL SELECT '2024-01-01T00:00:00.000000Z'::TIMESTAMP, -1, 'first', 0.0 FROM long_sequence(1)
                UNION ALL SELECT '2024-01-01T23:00:00.000000Z'::TIMESTAMP, -2, 'last', 0.0 FROM long_sequence(1)
                """);
        execute("INSERT INTO t VALUES ('2024-01-02T00:00:00.000000Z', -3, 'next day', 0.0)");
        drainWalQueue();
        execute("ALTER TABLE t CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
        drainWalQueue();
    }
}
