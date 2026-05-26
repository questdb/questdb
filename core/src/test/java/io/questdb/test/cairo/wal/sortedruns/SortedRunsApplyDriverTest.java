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

package io.questdb.test.cairo.wal.sortedruns;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.wal.sortedruns.SortedRunsFormat;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;

/**
 * Verifies that {@link SortedRunsApplyDriver} converts pending WAL transactions
 * into indexed-sorted-runs partitions with the expected {@code _sortedruns} content.
 */
public class SortedRunsApplyDriverTest extends AbstractCairoTest {

    @Test
    public void testFailsOnSymbolColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (ts TIMESTAMP, sym SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO x VALUES (1000, 'a')");
            final TableToken token = engine.verifyTableName("x");
            try (SortedRunsApplyDriver driver = new SortedRunsApplyDriver(engine)) {
                try {
                    driver.apply(token);
                    Assert.fail("expected exception for SYMBOL column");
                } catch (CairoException e) {
                    Assert.assertTrue(e.getMessage(), e.getMessage().contains("SYMBOL"));
                }
            }
        });
    }

    @Test
    public void testFailsOnVarcharColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (ts TIMESTAMP, name VARCHAR) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO x VALUES (1000, 'hello')");
            final TableToken token = engine.verifyTableName("x");
            try (SortedRunsApplyDriver driver = new SortedRunsApplyDriver(engine)) {
                try {
                    driver.apply(token);
                    Assert.fail("expected exception for VARCHAR column");
                } catch (CairoException e) {
                    Assert.assertTrue(e.getMessage(), e.getMessage().contains("variable-length"));
                }
            }
        });
    }

    @Test
    public void testMultipleInOrderCommits() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (ts TIMESTAMP, val LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO x VALUES (1000, 10), (2000, 20)");
            execute("INSERT INTO x VALUES (3000, 30), (4000, 40)");
            execute("INSERT INTO x VALUES (5000, 50)");
            final TableToken token = engine.verifyTableName("x");

            try (SortedRunsApplyDriver driver = new SortedRunsApplyDriver(engine)) {
                driver.apply(token);
            }

            final CommitsFileLocation location = locateCommitsFile(token, 0L);
            final List<SortedRunsDecoder.Record> records = SortedRunsDecoder.readAll(location.path, location.validSize);
            Assert.assertEquals(3, records.size());

            Assert.assertEquals(0L, records.get(0).physRowStart);
            Assert.assertEquals(2, records.get(0).rowCount);
            Assert.assertEquals(1000L, records.get(0).minTs);
            Assert.assertEquals(2000L, records.get(0).maxTs);

            Assert.assertEquals(2L, records.get(1).physRowStart);
            Assert.assertEquals(2, records.get(1).rowCount);
            Assert.assertEquals(3000L, records.get(1).minTs);
            Assert.assertEquals(4000L, records.get(1).maxTs);

            Assert.assertEquals(4L, records.get(2).physRowStart);
            Assert.assertEquals(1, records.get(2).rowCount);
            Assert.assertEquals(5000L, records.get(2).minTs);
            Assert.assertEquals(5000L, records.get(2).maxTs);

            for (SortedRunsDecoder.Record r : records) {
                Assert.assertTrue("SORTED_BY_TS expected", r.hasFlag(SortedRunsFormat.FLAG_SORTED_BY_TS));
                Assert.assertEquals(0, r.extLength);
            }
        });
    }

    @Test
    public void testO3CommitProducesSortedRun() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (ts TIMESTAMP, val LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            // Out-of-order timestamps within one transaction.
            execute("INSERT INTO x VALUES (3000, 30), (1000, 10), (2000, 20)");
            final TableToken token = engine.verifyTableName("x");

            try (SortedRunsApplyDriver driver = new SortedRunsApplyDriver(engine)) {
                driver.apply(token);
            }

            final CommitsFileLocation location = locateCommitsFile(token, 0L);
            final List<SortedRunsDecoder.Record> records = SortedRunsDecoder.readAll(location.path, location.validSize);
            Assert.assertEquals(1, records.size());
            Assert.assertEquals(0L, records.get(0).physRowStart);
            Assert.assertEquals(3, records.get(0).rowCount);
            Assert.assertEquals(1000L, records.get(0).minTs);
            Assert.assertEquals(3000L, records.get(0).maxTs);
            Assert.assertTrue(records.get(0).hasFlag(SortedRunsFormat.FLAG_SORTED_BY_TS));
        });
    }

    @Test
    public void testSingleInOrderCommit() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (ts TIMESTAMP, val LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO x VALUES (1000, 10), (2000, 20), (3000, 30)");
            final TableToken token = engine.verifyTableName("x");

            try (SortedRunsApplyDriver driver = new SortedRunsApplyDriver(engine)) {
                driver.apply(token);
            }

            final CommitsFileLocation location = locateCommitsFile(token, 0L);
            final List<SortedRunsDecoder.Record> records = SortedRunsDecoder.readAll(location.path, location.validSize);
            Assert.assertEquals(1, records.size());
            final SortedRunsDecoder.Record r = records.get(0);
            Assert.assertEquals(0L, r.physRowStart);
            Assert.assertEquals(3, r.rowCount);
            Assert.assertEquals(1000L, r.minTs);
            Assert.assertEquals(3000L, r.maxTs);
            Assert.assertTrue(r.hasFlag(SortedRunsFormat.FLAG_SORTED_BY_TS));
            Assert.assertEquals(0, r.extLength);
        });
    }

    private static CommitsFileLocation locateCommitsFile(TableToken token, long partitionTimestamp) throws IOException {
        final CharSequence root = engine.getConfiguration().getDbRoot();
        final int timestampType = ColumnType.TIMESTAMP_MICRO;
        final TimestampDriver driver = ColumnType.getTimestampDriver(timestampType);
        final long partitionFloor = driver.getPartitionFloorMethod(PartitionBy.DAY).floor(partitionTimestamp);

        final long nameTxn;
        final long validSize;
        try (Path tablePath = new Path();
             TxReader txReader = new TxReader(engine.getConfiguration().getFilesFacade())) {
            tablePath.of(root).concat(token).concat(TableUtils.TXN_FILE_NAME);
            txReader.ofRO(tablePath.$(), timestampType, PartitionBy.DAY);
            txReader.unsafeLoadAll();
            int partitionIndex = -1;
            for (int i = 0, n = txReader.getPartitionCount(); i < n; i++) {
                if (txReader.getPartitionTimestampByIndex(i) == partitionFloor) {
                    partitionIndex = i;
                    break;
                }
            }
            Assert.assertTrue("partition " + partitionFloor + " not found", partitionIndex >= 0);
            nameTxn = txReader.getPartitionNameTxn(partitionIndex);
            Assert.assertTrue("sorted-runs flag not set", txReader.isPartitionSortedRuns(partitionIndex));
            validSize = txReader.getPartitionSortedRunsFileSize(partitionIndex);
        }

        try (Path path = new Path()) {
            path.of(root).concat(token);
            TableUtils.setPathForNativePartition(path, timestampType, PartitionBy.DAY, partitionFloor, nameTxn);
            path.concat(SortedRunsFormat.FILE_NAME);
            return new CommitsFileLocation(Paths.get(path.toString()), validSize);
        }
    }

    private static final class CommitsFileLocation {
        final java.nio.file.Path path;
        final long validSize;

        CommitsFileLocation(java.nio.file.Path path, long validSize) {
            this.path = path;
            this.validSize = validSize;
        }
    }
}
