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
 * {@code O3PartitionJob#writeFreshParquetFromO3} on a composite table -- the path that emits a fresh
 * parquet file straight from the O3 buffers for a brand-new partition on a FORMAT PARQUET table.
 * <p>
 * This class used to lock a REFUSAL, on the reasoning that the method built its target with the bare
 * {@code setPathForParquetPartition} overload and never consulted the {@code cellSegment} its caller
 * had in scope -- so on a composite table it would write at the bare day path, the same cell-blind
 * shape that makes {@code CONVERT PARTITION TO PARQUET} crash the JVM with a SIGBUS in the encoder.
 * Those paths were made cell-aware later, and the refusal outlived its reason: it stood only because
 * "nothing else has been audited for an all-parquet composite table".
 * <p>
 * That audit is {@link CompositeFormatParquetTest}. It found exactly one real defect -- four
 * cellKey-0 setters in {@code TableWriter#o3ConsumePartitionUpdateSink}'s brand-new-parquet branch,
 * whose symptom was a correctly written, correctly PLACED parquet cell that could not be read back --
 * and with that fixed the feature works. So this class now locks the positive: a composite FORMAT
 * PARQUET table drives the fresh-parquet write and reads its rows back.
 * <p>
 * {@link #testPlainFormatParquetStillWrites()} remains the control on the other side: it proves the
 * plain path still works, so a green composite result cannot come from FORMAT PARQUET having quietly
 * stopped doing anything at all.
 */
public class CompositeFreshParquetGateTest extends AbstractCairoTest {

    /**
     * A composite FORMAT PARQUET table takes the fresh-parquet write path and reads back.
     * <p>
     * Two cells in one day, deliberately: the defect this replaced was a per-cell record being
     * written by a by-timestamp setter, which is invisible with a single cell per day because there
     * is then only one record for the timestamp to resolve to.
     */
    @Test
    public void testCompositeFormatParquetWritesAndReadsBack() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE c (ts TIMESTAMP, exch SYMBOL, px DOUBLE) TIMESTAMP(ts) "
                    + "PARTITION BY DAY, exch FORMAT PARQUET WAL");
            execute("INSERT INTO c VALUES ('2023-01-01T01:00:00.000000Z','E0',1.0),"
                    + "('2023-01-01T02:00:00.000000Z','E1',2.0),"
                    + "('2023-01-01T03:00:00.000000Z','E0',3.0)");
            drainWalQueue();
            assertQuery("SELECT count() FROM c")
                    .noLeakCheck().noRandomAccess().expectSize()
                    .returns("count\n3\n");
            // Read the VALUES back, not only the count: the defect this replaced left the rows on disk
            // and intact while making the partition unreadable, so a count that comes from _txn rather
            // than from the cells would not have caught it.
            assertQuery("SELECT exch, sum(px) FROM c ORDER BY exch")
                    .noLeakCheck().expectSize()
                    .returns("exch\tsum\nE0\t4.0\nE1\t2.0\n");
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
