/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.cairo.NativeTimestampFinder;
import io.questdb.cairo.ParquetTimestampFinder;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableWriter;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import static io.questdb.std.Vect.BIN_SEARCH_SCAN_DOWN;
import static io.questdb.std.Vect.BIN_SEARCH_SCAN_UP;

public class TimestampFinderTest extends AbstractCairoTest {

    // TODO(puzpuzpuz): enable the test when ParquetTimestampFinder#findTimestamp() is done
    @Ignore
    @Test
    public void testFuzz() throws Exception {
        final Rnd rnd = TestUtils.generateRandom(LOG);
        final int rowCount = 10000;
        assertMemoryLeak(() -> {
            TableModel modelX = new TableModel(configuration, "x", PartitionBy.YEAR).timestamp();
            AbstractCairoTest.create(modelX);
            TableModel modelY = new TableModel(configuration, "y", PartitionBy.YEAR).timestamp();
            AbstractCairoTest.create(modelY);

            final long minTimestamp = TimestampFormatUtils.parseTimestamp("1980-01-01T00:00:00.000Z");
            long maxTimestamp = minTimestamp;
            long timestamp = minTimestamp;
            try (
                    TableWriter writerX = newOffPoolWriter(configuration, "x", metrics);
                    TableWriter writerY = newOffPoolWriter(configuration, "y", metrics)
            ) {
                for (int i = 0; i < rowCount; i++) {
                    writerX.newRow(timestamp).append();
                    writerY.newRow(timestamp).append();
                    maxTimestamp = timestamp;
                    timestamp += rnd.nextLong(Timestamps.MINUTE_MICROS);
                }
                writerX.commit();
                writerY.commit();
            }

            // convert y to parquet
            ddl("alter table y convert partition to parquet where timestamp >= 0");

            NativeTimestampFinder finderX = new NativeTimestampFinder();
            try (
                    TableReader readerX = newOffPoolReader(configuration, "x");
                    TableReader readerY = newOffPoolReader(configuration, "y");
                    ParquetTimestampFinder finderY = new ParquetTimestampFinder();
            ) {
                Assert.assertEquals(1, readerX.getPartitionCount());
                Assert.assertEquals(1, readerY.getPartitionCount());

                readerX.openPartition(0);
                readerY.openPartition(0);

                finderX.of(readerX, 0, 0, rowCount);
                finderY.of(readerY, 0, 0);

                Assert.assertEquals(minTimestamp, finderX.minTimestamp());
                Assert.assertEquals(finderX.minTimestamp(), finderY.minTimestamp());
                Assert.assertEquals(maxTimestamp, finderX.maxTimestamp());
                Assert.assertEquals(finderX.maxTimestamp(), finderY.maxTimestamp());

                for (int row = 0; row < rowCount; row++) {
                    Assert.assertEquals(finderX.timestampAt(row), finderY.timestampAt(row));
                }

                for (long ts = minTimestamp - 1; ts < maxTimestamp + 1; ts++) {
                    // full partition
                    Assert.assertEquals(
                            finderX.findTimestamp(ts, 0, rowCount, BIN_SEARCH_SCAN_UP),
                            finderY.findTimestamp(ts, 0, rowCount, BIN_SEARCH_SCAN_UP)
                    );
                    Assert.assertEquals(
                            finderX.findTimestamp(ts, 0, rowCount, BIN_SEARCH_SCAN_DOWN),
                            finderY.findTimestamp(ts, 0, rowCount, BIN_SEARCH_SCAN_DOWN)
                    );

                    // first partition half
                    Assert.assertEquals(
                            finderX.findTimestamp(ts, 0, rowCount / 2, BIN_SEARCH_SCAN_UP),
                            finderY.findTimestamp(ts, 0, rowCount / 2, BIN_SEARCH_SCAN_UP)
                    );
                    Assert.assertEquals(
                            finderX.findTimestamp(ts, 0, rowCount / 2, BIN_SEARCH_SCAN_DOWN),
                            finderY.findTimestamp(ts, 0, rowCount / 2, BIN_SEARCH_SCAN_DOWN)
                    );

                    // second partition half
                    Assert.assertEquals(
                            finderX.findTimestamp(ts, rowCount / 2, rowCount, BIN_SEARCH_SCAN_UP),
                            finderY.findTimestamp(ts, rowCount / 2, rowCount, BIN_SEARCH_SCAN_UP)
                    );
                    Assert.assertEquals(
                            finderX.findTimestamp(ts, rowCount / 2, rowCount, BIN_SEARCH_SCAN_DOWN),
                            finderY.findTimestamp(ts, rowCount / 2, rowCount, BIN_SEARCH_SCAN_DOWN)
                    );

                    // partition middle
                    Assert.assertEquals(
                            finderX.findTimestamp(ts, rowCount / 3, 2 * rowCount / 3, BIN_SEARCH_SCAN_UP),
                            finderY.findTimestamp(ts, rowCount / 3, 2 * rowCount / 3, BIN_SEARCH_SCAN_UP)
                    );
                    Assert.assertEquals(
                            finderX.findTimestamp(ts, rowCount / 3, 2 * rowCount / 3, BIN_SEARCH_SCAN_DOWN),
                            finderY.findTimestamp(ts, rowCount / 3, 2 * rowCount / 3, BIN_SEARCH_SCAN_DOWN)
                    );
                }
            }
        });
    }
}
