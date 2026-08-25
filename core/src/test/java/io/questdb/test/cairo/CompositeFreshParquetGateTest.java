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

package io.questdb.test.cairo;

import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Regression lock for the composite guard on {@code O3PartitionJob#writeFreshParquetFromO3}.
 * <p>
 * That method emits a fresh parquet file straight from the O3 buffers for a brand-new partition on a
 * FORMAT PARQUET table. It builds its target with the bare {@code setPathForParquetPartition}
 * overload and never consults the {@code cellSegment} its own CALLER has in scope, so on a composite
 * table it would write at the bare day path -- the identical cell-blind shape that makes
 * {@code CONVERT PARTITION TO PARQUET} crash the JVM with a SIGBUS inside the native encoder.
 * <p>
 * It is unreachable today only because FORMAT PARQUET is refused earlier, at CREATE. This test pins
 * BOTH halves of that claim, because a shadow is not a guarantee: the two gates lifted in this
 * session (ATTACH, DROP COLUMN) each exposed a site of exactly this kind the moment their outer gate
 * came off.
 * <p>
 * {@link #testPlainFormatParquetStillWrites()} is the POSITIVE CONTROL. Without it the composite
 * assertion is vacuous -- it would pass simply because the shape never drives a fresh-parquet write
 * at all, which is precisely how an earlier probe in this area produced a green result against a
 * live defect.
 */
public class CompositeFreshParquetGateTest extends AbstractCairoTest {

    /**
     * Invariant 6: FORMAT PARQUET must be refused AT THE STATEMENT on a composite table.
     * <p>
     * MEASURED 2026-08-26 before this was fixed: the CREATE was accepted and the table then SUSPENDED
     * on the first INSERT via the writer-side gate in processO3Block --
     * {@code suspended=true, errorMessage=composite partitioning does not yet support FORMAT PARQUET},
     * table holding 0 rows, and SHOW CREATE TABLE still advertising FORMAT PARQUET. A successful DDL
     * and a broken table.
     */
    @Test
    public void testCompositeFormatParquetRefusedAtCreate() throws Exception {
        assertMemoryLeak(() -> {
            try {
                execute("CREATE TABLE c (ts TIMESTAMP, exch SYMBOL, px DOUBLE) TIMESTAMP(ts) "
                        + "PARTITION BY DAY, exch FORMAT PARQUET WAL");
                Assert.fail("FORMAT PARQUET must be refused on a composite table");
            } catch (Exception e) {
                TestUtils.assertContains(e.getMessage(),
                        "composite partitioning does not yet support FORMAT PARQUET");
            }
            // the refusal is at the statement, so nothing was created to be broken
            assertQuery("SELECT count() FROM tables() WHERE table_name = 'c'")
                    .noLeakCheck().noRandomAccess().expectSize()
                    .returns("count\n0\n");
        });
    }

    /**
     * POSITIVE CONTROL: a plain FORMAT PARQUET table really does take the fresh-parquet write path and
     * reads its rows back. If this ever stops holding, the composite assertion above stops meaning
     * anything and this suite must be re-shaped rather than trusted.
     */
    @Test
    public void testPlainFormatParquetStillWrites() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE p (ts TIMESTAMP, exch SYMBOL, px DOUBLE) TIMESTAMP(ts) "
                    + "PARTITION BY DAY FORMAT PARQUET WAL");
            execute("INSERT INTO p VALUES ('2024-01-01T00:00:00.000000Z','BTC',1.0),"
                    + "('2024-01-02T00:00:00.000000Z','ETH',2.0)");
            drainWalQueue();

            assertQuery("SELECT count() FROM p")
                    .noLeakCheck().noRandomAccess().expectSize()
                    .returns("count\n2\n");
            // and the partitions really are parquet -- otherwise this drove the ordinary native path
            // and controls nothing
            assertQuery("SELECT count() FROM table_partitions('p') WHERE isParquet = true")
                    .noLeakCheck().noRandomAccess().expectSize()
                    .returns("count\n2\n");
        });
    }
}
