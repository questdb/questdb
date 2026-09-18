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

package io.questdb.test.cairo.o3;

import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TxReader;
import io.questdb.std.FilesFacade;
import io.questdb.std.NumericException;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Once a parquet partition becomes the last one, the writer has no partition to append
 * into. Before the fix it kept the previous native last partition open with append offsets
 * that later mid-partition O3 appends (which write through their own file descriptors)
 * silently outgrew, and the next truncating close of the writer trimmed every column file
 * back to ceilPageSize(staleOffset), discarding the appended rows. A reader or column type
 * converter that then mapped the committed row count faulted past the shortened file.
 * <p>
 * Both tests here make the day-1 partition native, grow it from 500 to 600 rows through the
 * mid-partition O3 append while a parquet day-2 partition is the last one, close the writer
 * and check the column file length and the data. For a LONG column 500 rows fit into one
 * 4 KiB page and 600 rows need two, so a truncating close with a stale 500-row offset is
 * observable as a 4096-byte file regardless of the platform page size.
 */
public class O3ParquetLastPartitionTest extends AbstractCairoTest {

    @Test
    public void testO3AppendIntoNativePartitionAfterColumnTypePrepassSurvivesWriterClose() throws Exception {
        // The sequence WalWriterFuzzTest#testCreateTableAsParquet hit in CI: the column type change
        // decodes the parquet last partition to native and the writer opens it as its active
        // partition, a later parquet partition is born, an O3 append grows the native partition
        // through the mid-partition path, the pool closes the writer and the next column type
        // change maps the committed row count over the shortened file.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (ts TIMESTAMP, a LONG, s STRING) TIMESTAMP(ts) PARTITION BY DAY FORMAT PARQUET WAL");
            execute("INSERT INTO x SELECT timestamp_sequence('2022-02-25T00:00:00', 1_000_000L), x, 's' || x FROM long_sequence(500)");
            drainWalQueue();

            // parquet -> native pre-pass on the last partition; the writer opens it as active
            execute("ALTER TABLE x ALTER COLUMN s TYPE SYMBOL");
            drainWalQueue();

            appendDay2ThenDay1AndCloseWriter();

            // the operation that crashed the JVM in CI
            execute("ALTER TABLE x ALTER COLUMN a TYPE INT");
            drainWalQueue();
            engine.releaseInactive();

            assertQuery("SELECT count(), min(a), max(a), sum(a) FROM x WHERE ts IN '2022-02-25'")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count\tmin\tmax\tsum
                            600\t1\t500\t130300
                            """);
        });
    }

    @Test
    public void testO3AppendIntoNativePartitionBehindParquetLastSurvivesWriterClose() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (ts TIMESTAMP, a LONG, s STRING) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO x SELECT timestamp_sequence('2022-02-25T00:00:00', 1_000_000L), x, 's' || x FROM long_sequence(500)");
            drainWalQueue();

            // every partition born after this is parquet; the writer holds native day 1 open
            execute("ALTER TABLE x SET FORMAT PARQUET");
            drainWalQueue();

            appendDay2ThenDay1AndCloseWriter();
        });
    }

    private void appendDay2ThenDay1AndCloseWriter() throws Exception {
        // day 2 is born parquet and becomes the last partition
        execute("INSERT INTO x SELECT timestamp_sequence('2022-02-26T00:00:00', 1_000_000L), x, 's' || x FROM long_sequence(10)");
        drainWalQueue();

        // rows after day 1's max timestamp: in-place mid-partition O3 append, 500 -> 600 rows
        execute("INSERT INTO x SELECT timestamp_sequence('2022-02-25T10:00:00', 1_000_000L), x, 's' || x FROM long_sequence(100)");
        drainWalQueue();

        // pool closes the writer: doClose -> freeColumns(true) -> MemoryCMARWImpl.close(true)
        engine.releaseInactive();

        long length = columnFileLength("x", "2022-02-25T00:00:00.000000Z", "a");
        Assert.assertTrue(
                "writer close truncated the appended native partition column file to " + length + " bytes",
                length >= 600L * Long.BYTES
        );

        // 1..500 from the first insert plus 1..100 from the appended rows
        assertQuery("SELECT count(), min(a), max(a), sum(a) FROM x WHERE ts IN '2022-02-25'")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        count\tmin\tmax\tsum
                        600\t1\t500\t130300
                        """);
        // the last appended rows sit in the region a truncating close would have discarded
        assertQuery("SELECT ts, a, s FROM x WHERE ts >= '2022-02-25T10:01:38' AND ts IN '2022-02-25'")
                .timestamp("ts")
                .returns("""
                        ts\ta\ts
                        2022-02-25T10:01:38.000000Z\t99\ts99
                        2022-02-25T10:01:39.000000Z\t100\ts100
                        """);
    }

    private static long columnFileLength(String tableName, String partitionTimestamp, String columnName) throws NumericException {
        final TableToken token = engine.verifyTableName(tableName);
        final FilesFacade ff = engine.getConfiguration().getFilesFacade();
        final long ts = MicrosFormatUtils.parseUTCTimestamp(partitionTimestamp);
        try (TableReader reader = engine.getReader(token); Path path = new Path()) {
            final TxReader tx = reader.getTxFile();
            final int partitionIndex = tx.getPartitionIndex(ts);
            Assert.assertTrue("partition not found: " + partitionTimestamp, partitionIndex > -1);
            final int columnIndex = reader.getMetadata().getColumnIndex(columnName);
            final long columnNameTxn = reader.getColumnVersionReader().getColumnNameTxn(ts, columnIndex);
            path.of(engine.getConfiguration().getDbRoot()).concat(token);
            TableUtils.setPathForNativePartition(
                    path,
                    reader.getMetadata().getTimestampType(),
                    PartitionBy.DAY,
                    ts,
                    tx.getPartitionNameTxn(partitionIndex)
            );
            final long length = ff.length(TableUtils.dFile(path, columnName, columnNameTxn));
            LOG.info().$("column file [path=").$(path).$(", length=").$(length).I$();
            return length;
        }
    }
}
