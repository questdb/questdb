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

import io.questdb.PropertyKey;
import io.questdb.cairo.NativeTimestampFinder;
import io.questdb.cairo.ParquetTimestampFinder;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableWriter;
import io.questdb.griffin.engine.table.parquet.PartitionDecoder;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class TimestampFinderTest extends AbstractCairoTest {

    @Override
    public void setUp() {
        super.setUp();
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
    }

    @Test
    public void testFuzzAllDuplicates() throws Exception {
        testFuzz(1000, 1000);
    }

    @Test
    public void testFuzzFewDuplicates() throws Exception {
        testFuzz(1000, 1);
    }

    @Test
    public void testFuzzSomeDuplicates() throws Exception {
        testFuzz(1000, 100);
    }

    private void testFuzz(int rowCount, int duplicatesPerTick) throws Exception {
        final Rnd rnd = TestUtils.generateRandom(LOG);
        assertMemoryLeak(() -> {
            TableModel oracleModel = new TableModel(configuration, "oracle", PartitionBy.YEAR).timestamp();
            AbstractCairoTest.create(oracleModel);
            TableModel model = new TableModel(configuration, "x", PartitionBy.YEAR).timestamp();
            AbstractCairoTest.create(model);

            final long minTimestamp = TimestampFormatUtils.parseTimestamp("1980-01-01T00:00:00.000Z");
            long maxTimestamp = minTimestamp;
            long timestamp = minTimestamp;
            try (
                    TableWriter oracleWriter = newOffPoolWriter(configuration, "oracle");
                    TableWriter writer = newOffPoolWriter(configuration, "x")
            ) {
                int ticks = duplicatesPerTick;
                for (int i = 0; i < rowCount; i++) {
                    oracleWriter.newRow(timestamp).append();
                    writer.newRow(timestamp).append();
                    maxTimestamp = timestamp;
                    if (--ticks == 0) {
                        if (duplicatesPerTick > 1) {
                            // we want to be in control of the number of duplicates
                            timestamp += (rnd.nextLong(1) + 1) * Timestamps.MINUTE_MICROS;
                        } else {
                            // extra duplicates are fine
                            timestamp += rnd.nextLong(2) * Timestamps.MINUTE_MICROS;
                        }
                        ticks = duplicatesPerTick;
                    }
                }

                // write one more row, so that the active partition contains it;
                // that's because we can't convert active partition to parquet
                long newerTimestamp = TimestampFormatUtils.parseTimestamp("2000-01-01T00:00:00.000Z");
                oracleWriter.newRow(newerTimestamp).append();
                writer.newRow(newerTimestamp).append();

                oracleWriter.commit();
                writer.commit();
            }

            // convert x to parquet
            execute("alter table x convert partition to parquet where timestamp >= 0");

            NativeTimestampFinder oracleFinder = new NativeTimestampFinder();
            try (
                    TableReader oracleReader = newOffPoolReader(configuration, "oracle");
                    TableReader reader = newOffPoolReader(configuration, "x");
                    PartitionDecoder partitionDecoder = new PartitionDecoder();
                    ParquetTimestampFinder finder = new ParquetTimestampFinder(partitionDecoder)
            ) {
                Assert.assertEquals(2, oracleReader.getPartitionCount());
                Assert.assertEquals(2, reader.getPartitionCount());

                oracleReader.openPartition(0);
                reader.openPartition(0);

                oracleFinder.of(oracleReader, 0, 0, rowCount);
                finder.of(reader, 0, 0);

                Assert.assertEquals(minTimestamp, oracleFinder.minTimestamp());
                Assert.assertEquals(oracleFinder.minTimestamp(), finder.minTimestamp());
                Assert.assertEquals(maxTimestamp, oracleFinder.maxTimestamp());
                Assert.assertEquals(oracleFinder.maxTimestamp(), finder.maxTimestamp());

                for (int row = 0; row < rowCount; row++) {
                    Assert.assertEquals(oracleFinder.timestampAt(row), finder.timestampAt(row));
                }

                final long start = System.nanoTime();
                long calls = 0;
                for (long ts = minTimestamp - Timestamps.MINUTE_MICROS; ts < maxTimestamp + Timestamps.MINUTE_MICROS; ts += Timestamps.MINUTE_MICROS) {
                    // full partition
                    Assert.assertEquals(
                            oracleFinder.findTimestamp(ts, 0, rowCount - 1),
                            finder.findTimestamp(ts, 0, rowCount - 1)
                    );

                    // first partition half
                    Assert.assertEquals(
                            oracleFinder.findTimestamp(ts, 0, rowCount / 2),
                            finder.findTimestamp(ts, 0, rowCount / 2)
                    );

                    // second partition half
                    Assert.assertEquals(
                            oracleFinder.findTimestamp(ts, rowCount / 2, rowCount - 1),
                            finder.findTimestamp(ts, rowCount / 2, rowCount - 1)
                    );

                    // partition middle
                    Assert.assertEquals(
                            oracleFinder.findTimestamp(ts, rowCount / 3, 2L * rowCount / 3),
                            finder.findTimestamp(ts, rowCount / 3, 2L * rowCount / 3)
                    );

                    calls += 8;
                }

                System.out.println("average call latency: " + ((System.nanoTime() - start) / calls) + "ns");
            }
        });
    }
}
