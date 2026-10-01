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

package io.questdb.test.cairo;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TxWriter;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * A remotely served partition keeps only the {@code _pm} sidecar on local disk, so a read that
 * must decode its rows reaches the parquet decoder with no file bytes and fails with
 * the defined decode-time error instead of crashing or returning wrong data.
 */
public class RemotelyServedPartitionReadTest extends AbstractCairoTest {

    @Test
    public void testReadFailsOnRemotelyServedPartitionWithoutDataParquet() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t AS (
                        SELECT x::INT id, timestamp_sequence('2024-06-10', 24 * 3600 * 1_000_000L) ts
                        FROM long_sequence(3)
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL""");
            // converts the two inactive partitions; 2024-06-12 stays native
            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET WHERE ts > 0");

            final TableToken token = engine.verifyTableName("t");
            long partitionTs;
            long partitionNameTxn;
            // stamp the first partition remotely served: REMOTE set, parquet_generated clear
            try (TableWriter writer = getWriter("t")) {
                TxWriter tx = writer.getTxWriter();
                Assert.assertTrue("first partition must be parquet", tx.isPartitionParquet(0));
                Assert.assertTrue("second partition must be parquet", tx.isPartitionParquet(1));
                Assert.assertFalse("third (active) partition must be native", tx.isPartitionParquet(2));
                partitionTs = tx.getPartitionTimestampByIndex(0);
                partitionNameTxn = tx.getPartitionNameTxn(0);
                tx.setPartitionParquetGenerated(0, false);
                tx.setPartitionRemote(0, true);
                tx.bumpPartitionTableVersion();
                tx.commit(writer.getDenseSymbolMapWriters());
                Assert.assertTrue("first partition must be remotely served", tx.isPartitionRemotelyServed(0));
            }

            // the remotely served on-disk shape: _pm stays, data.parquet is gone
            final FilesFacade ff = configuration.getFilesFacade();
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(token);
                TableUtils.setPathForParquetPartition(path, ColumnType.TIMESTAMP, PartitionBy.DAY, partitionTs, partitionNameTxn);
                Assert.assertTrue("data.parquet must exist after the convert", ff.exists(path.$()));
                Assert.assertTrue("data.parquet must be removed", ff.removeQuiet(path.$()));

                path.of(configuration.getDbRoot()).concat(token);
                TableUtils.setPathForParquetPartitionMetadata(path, ColumnType.TIMESTAMP, PartitionBy.DAY, partitionTs, partitionNameTxn);
                Assert.assertTrue("_pm must stay", ff.exists(path.$()));
            }

            // a full scan opens the partition off _pm and reaches the row-group
            // decoder holding a null parquet file pointer
            assertExceptionNoLeakCheck("SELECT id FROM t", -1, "parquet file pointer is null or size is zero");

            // control: a read over the local parquet and native partitions still works
            assertQuery("SELECT * FROM t WHERE ts >= '2024-06-11'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            id\tts
                            2\t2024-06-11T00:00:00.000000Z
                            3\t2024-06-12T00:00:00.000000Z
                            """);
        });
    }
}
