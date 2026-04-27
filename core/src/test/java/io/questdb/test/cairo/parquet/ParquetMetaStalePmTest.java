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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests stale _pm detection and writer-side regeneration.
 * <p>
 * A _pm file becomes stale when old code modifies data.parquet without
 * updating the _pm (e.g., after a version rollback). The writer detects
 * this via exact parquet-file-size matching in the footer chain and
 * regenerates the _pm from data.parquet.
 */
public class ParquetMetaStalePmTest extends AbstractCairoTest {

    @Test
    public void testReaderDoesNotCrashOnStalePm() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            execute("CREATE TABLE t (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES(1, '2024-06-10T00:00:00.000000Z')");
            execute("INSERT INTO t VALUES(2, '2024-06-11T00:00:00.000000Z')");
            execute("INSERT INTO t VALUES(3, '2024-06-12T00:00:00.000000Z')");
            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET LIST '2024-06-10'");

            corruptPm("t");

            // The non-parquet partition (2024-06-11, 2024-06-12) should still be
            // accessible. A query targeting only native data must not crash.
            assertSql("id\tts\n2\t2024-06-11T00:00:00.000000Z\n", "SELECT * FROM t WHERE ts = '2024-06-11'");
        });
    }

    @Test
    public void testStalePmWithMultiplePartitions() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            execute("CREATE TABLE t (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                    (1, '2024-06-10T00:00:00.000000Z'),
                    (2, '2024-06-11T00:00:00.000000Z'),
                    (3, '2024-06-12T00:00:00.000000Z'),
                    (4, '2024-06-13T00:00:00.000000Z')
                    """);
            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET WHERE ts > 0");

            // Corrupt only the first parquet partition's _pm.
            final TableToken token = engine.verifyTableName("t");
            try (TableReader reader = engine.getReader(token)) {
                long ts = reader.getTxFile().getPartitionTimestampByIndex(0);
                long nameTxn = reader.getTxFile().getPartitionNameTxn(0);
                corruptPmByTimestamp(token, ts, nameTxn);
            }

            // Convert all parquet back to native — writer regenerates the stale one.
            execute("ALTER TABLE t CONVERT PARTITION TO NATIVE WHERE ts > 0");

            assertSql("count\n4\n", "SELECT count() FROM t");
        });
    }

    @Test
    public void testWriterFixesStalePmThenReaderWorks() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            execute("CREATE TABLE t (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES(1, '2024-06-10T00:00:00.000000Z')");
            execute("INSERT INTO t VALUES(2, '2024-06-11T00:00:00.000000Z')");
            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET LIST '2024-06-10'");

            corruptPm("t");

            // Writer converts back to native, regenerating the stale _pm internally.
            execute("ALTER TABLE t CONVERT PARTITION TO NATIVE LIST '2024-06-10'");

            // Now a fresh reader sees all data.
            assertSql("count\n2\n", "SELECT count() FROM t");
            assertSql("id\tts\n1\t2024-06-10T00:00:00.000000Z\n", "SELECT * FROM t WHERE ts = '2024-06-10'");
        });
    }

    @Test
    public void testWriterRegeneratesStalePmOnConvertToNative() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            execute("CREATE TABLE t (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES(1, '2024-06-10T00:00:00.000000Z')");
            execute("INSERT INTO t VALUES(2, '2024-06-11T00:00:00.000000Z')");
            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET LIST '2024-06-10'");

            corruptPm("t");

            // Convert back to native — writer must regenerate stale _pm first.
            execute("ALTER TABLE t CONVERT PARTITION TO NATIVE LIST '2024-06-10'");

            // Data should be intact.
            assertSql("id\tts\n1\t2024-06-10T00:00:00.000000Z\n", "SELECT * FROM t WHERE ts = '2024-06-10'");
        });
    }

    private void corruptPm(String tableName) throws Exception {
        final TableToken token = engine.verifyTableName(tableName);

        // Collect partition info before releasing caches.
        long[] timestamps;
        long[] nameTxns;
        try (TableReader reader = engine.getReader(token)) {
            int count = 0;
            for (int i = 0, n = reader.getPartitionCount(); i < n; i++) {
                if (reader.getPartitionFormat(i) == PartitionFormat.PARQUET) {
                    count++;
                }
            }
            timestamps = new long[count];
            nameTxns = new long[count];
            int idx = 0;
            for (int i = 0, n = reader.getPartitionCount(); i < n; i++) {
                if (reader.getPartitionFormat(i) == PartitionFormat.PARQUET) {
                    timestamps[idx] = reader.getTxFile().getPartitionTimestampByIndex(i);
                    nameTxns[idx] = reader.getTxFile().getPartitionNameTxn(i);
                    idx++;
                }
            }
        }

        // Release all caches so mmaps are unmapped.
        engine.releaseAllWriters();
        engine.releaseAllReaders();
        engine.releaseInactive();

        // Corrupt _pm files on disk.
        for (int i = 0; i < timestamps.length; i++) {
            corruptPmByTimestamp(token, timestamps[i], nameTxns[i]);
        }
    }

    private void corruptPmByTimestamp(TableToken token, long partitionTs, long partitionNameTxn) {
        final FilesFacade ff = configuration.getFilesFacade();
        try (Path path = new Path()) {
            path.of(configuration.getDbRoot()).concat(token);
            TableUtils.setPathForParquetPartitionMetadata(path, ColumnType.TIMESTAMP, PartitionBy.DAY, partitionTs, partitionNameTxn);
            long fd = ff.openRW(path.$(), 0);
            Assert.assertTrue("could not open _pm for corruption: " + path, fd >= 0);
            try {
                Assert.assertTrue("truncate failed", ff.truncate(fd, 0));
            } finally {
                ff.close(fd);
            }
        }
    }
}
