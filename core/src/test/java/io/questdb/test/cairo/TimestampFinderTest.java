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

import io.questdb.PropertyKey;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.NativeTimestampFinder;
import io.questdb.cairo.ParquetTimestampFinder;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TimestampDriver;
import io.questdb.griffin.engine.table.parquet.ParquetPartitionDecoder;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class TimestampFinderTest extends AbstractCairoTest {
    private final boolean enableParquetStatistics;

    public TimestampFinderTest(boolean enableParquetStatistics) {
        this.enableParquetStatistics = enableParquetStatistics;
    }

    @Parameterized.Parameters(name = "enableParquetStatistics={0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {true},
                {false},
        });
    }

    @Override
    public void setUp() {
        super.setUp();
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_STATISTICS_ENABLED, enableParquetStatistics);
    }

    @Test
    public void testCountsAcrossDuplicateRuns() throws Exception {
        assertMemoryLeak(() -> {
            for (int timestampType : new int[]{ColumnType.TIMESTAMP_MICRO, ColumnType.TIMESTAMP_NANO}) {
                final TimestampDriver driver = ColumnType.getTimestampDriver(timestampType);
                final long start = driver.parseFloorLiteral("1980-01-01T00:00:00.000Z");
                assertCounts(
                        timestampType,
                        new long[]{
                                start,
                                start,
                                start,
                                start + 10,
                                start + 10,
                                start + 20,
                                start + 20,
                                start + 20,
                                start + 20,
                                start + 30
                        },
                        new long[]{
                                Long.MIN_VALUE,
                                start - 1,
                                start,
                                start + 1,
                                start + 9,
                                start + 10,
                                start + 11,
                                start + 19,
                                start + 20,
                                start + 21,
                                start + 29,
                                start + 30,
                                start + 31,
                                Long.MAX_VALUE
                        }
                );
            }
        });
    }

    @Test
    public void testCountsAtParquetRowGroupBoundaries() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
            for (int timestampType : new int[]{ColumnType.TIMESTAMP_MICRO, ColumnType.TIMESTAMP_NANO}) {
                final TimestampDriver driver = ColumnType.getTimestampDriver(timestampType);
                final long start = driver.parseFloorLiteral("1980-01-01T00:00:00.000Z");
                assertCounts(
                        timestampType,
                        new long[]{
                                start,
                                start + 1,
                                start + 2,
                                start + 10,
                                start + 10,
                                start + 10,
                                start + 10,
                                start + 10,
                                start + 20,
                                start + 21,
                                start + 22,
                                start + 23
                        },
                        new long[]{
                                start - 1,
                                start,
                                start + 2,
                                start + 3,
                                start + 9,
                                start + 10,
                                start + 11,
                                start + 19,
                                start + 20,
                                start + 23,
                                start + 24
                        }
                );
            }
        });
    }

    @Test
    public void testCountsAtTimestampExtremes() throws Exception {
        assertMemoryLeak(() -> {
            for (int timestampType : new int[]{ColumnType.TIMESTAMP_MICRO, ColumnType.TIMESTAMP_NANO}) {
                final TimestampDriver driver = ColumnType.getTimestampDriver(timestampType);
                final long start = driver.parseFloorLiteral("1980-01-01T00:00:00.000Z");
                assertCounts(
                        timestampType,
                        new long[]{start, start + 1, start + 2},
                        new long[]{
                                Long.MIN_VALUE,
                                Long.MIN_VALUE + 1,
                                -1,
                                0,
                                start - 1,
                                start,
                                start + 1,
                                start + 2,
                                start + 3,
                                Long.MAX_VALUE - 1,
                                Long.MAX_VALUE
                        }
                );
            }
        });
    }

    @Test
    public void testCountsInGaps() throws Exception {
        assertMemoryLeak(() -> {
            for (int timestampType : new int[]{ColumnType.TIMESTAMP_MICRO, ColumnType.TIMESTAMP_NANO}) {
                final TimestampDriver driver = ColumnType.getTimestampDriver(timestampType);
                final long start = driver.parseFloorLiteral("1980-01-01T00:00:00.000Z");
                assertCounts(
                        timestampType,
                        new long[]{start, start + 100, start + 1_000, start + 10_000},
                        new long[]{
                                start - 1,
                                start,
                                start + 1,
                                start + 50,
                                start + 99,
                                start + 100,
                                start + 101,
                                start + 999,
                                start + 1_000,
                                start + 1_001,
                                start + 9_999,
                                start + 10_000,
                                start + 10_001
                        }
                );
            }
        });
    }

    @Test
    public void testCountsSingleTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            for (int timestampType : new int[]{ColumnType.TIMESTAMP_MICRO, ColumnType.TIMESTAMP_NANO}) {
                final TimestampDriver driver = ColumnType.getTimestampDriver(timestampType);
                final long timestamp = driver.parseFloorLiteral("1980-01-01T00:00:00.000Z");
                assertCounts(
                        timestampType,
                        new long[]{timestamp},
                        new long[]{Long.MIN_VALUE, timestamp - 1, timestamp, timestamp + 1, Long.MAX_VALUE}
                );
            }
        });
    }

    @Test
    public void testEmptyFindersCountWithoutPrepare() throws Exception {
        assertMemoryLeak(() -> {
            final NativeTimestampFinder nativeFinder = new NativeTimestampFinder();
            try (
                    ParquetPartitionDecoder partitionDecoder = new ParquetPartitionDecoder();
                    ParquetTimestampFinder parquetFinder = new ParquetTimestampFinder(partitionDecoder)
            ) {
                for (long timestamp : new long[]{Long.MIN_VALUE, -1, 0, 1, Long.MAX_VALUE}) {
                    Assert.assertEquals(0, nativeFinder.countBefore(timestamp));
                    Assert.assertEquals(0, nativeFinder.countThrough(timestamp));
                    Assert.assertEquals(0, parquetFinder.countBefore(timestamp));
                    Assert.assertEquals(0, parquetFinder.countThrough(timestamp));
                }
            }
        });
    }

    @Test
    public void testFuzzAllDuplicates() throws Exception {
        testFuzz(1000);
    }

    @Test
    public void testFuzzFewDuplicates() throws Exception {
        testFuzz(1);
    }

    @Test
    public void testFuzzSomeDuplicates() throws Exception {
        testFuzz(100);
    }

    private void assertCounts(int timestampType, long[] timestamps, long[] boundaries) throws Exception {
        final String suffix = timestampType == ColumnType.TIMESTAMP_MICRO ? "micro" : "nano";
        final String oracleTable = "oracle_" + suffix;
        final String parquetTable = "x_" + suffix;
        AbstractCairoTest.create(
                new TableModel(configuration, oracleTable, PartitionBy.YEAR).timestamp(timestampType)
        );
        AbstractCairoTest.create(
                new TableModel(configuration, parquetTable, PartitionBy.YEAR).timestamp(timestampType)
        );

        final TimestampDriver driver = ColumnType.getTimestampDriver(timestampType);
        final long newerTimestamp = driver.parseFloorLiteral("2000-01-01T00:00:00.000Z");
        try (
                TableWriter oracleWriter = newOffPoolWriter(configuration, oracleTable);
                TableWriter parquetWriter = newOffPoolWriter(configuration, parquetTable)
        ) {
            for (long timestamp : timestamps) {
                oracleWriter.newRow(timestamp).append();
                parquetWriter.newRow(timestamp).append();
            }
            oracleWriter.newRow(newerTimestamp).append();
            parquetWriter.newRow(newerTimestamp).append();
            oracleWriter.commit();
            parquetWriter.commit();
        }

        execute("ALTER TABLE " + parquetTable + " CONVERT PARTITION TO PARQUET WHERE timestamp >= 0");

        final NativeTimestampFinder nativeFinder = new NativeTimestampFinder();
        try (
                TableReader nativeReader = newOffPoolReader(configuration, oracleTable);
                TableReader parquetReader = newOffPoolReader(configuration, parquetTable);
                ParquetPartitionDecoder partitionDecoder = new ParquetPartitionDecoder();
                ParquetTimestampFinder parquetFinder = new ParquetTimestampFinder(partitionDecoder)
        ) {
            Assert.assertEquals(2, nativeReader.getPartitionCount());
            Assert.assertEquals(2, parquetReader.getPartitionCount());
            Assert.assertEquals(timestamps.length, nativeReader.openPartition(0));
            Assert.assertEquals(timestamps.length, parquetReader.openPartition(0));

            nativeFinder.of(nativeReader, 0, 0, timestamps.length);
            parquetFinder.of(parquetReader, 0, 0);

            Assert.assertTrue(nativeFinder.minTimestampLowerBound() <= timestamps[0]);
            Assert.assertTrue(nativeFinder.maxTimestampUpperBound() >= timestamps[timestamps.length - 1]);
            Assert.assertTrue(parquetFinder.minTimestampLowerBound() <= timestamps[0]);
            Assert.assertTrue(parquetFinder.maxTimestampUpperBound() >= timestamps[timestamps.length - 1]);

            nativeFinder.prepare();
            parquetFinder.prepare();

            Assert.assertEquals(timestamps[0], nativeFinder.minTimestampExact());
            Assert.assertEquals(timestamps[0], parquetFinder.minTimestampExact());
            Assert.assertEquals(timestamps[timestamps.length - 1], nativeFinder.maxTimestampExact());
            Assert.assertEquals(timestamps[timestamps.length - 1], parquetFinder.maxTimestampExact());
            for (long boundary : boundaries) {
                final long expectedBefore = countBefore(timestamps, boundary);
                final long expectedThrough = countThrough(timestamps, boundary);
                Assert.assertEquals("native countBefore(" + boundary + ')', expectedBefore, nativeFinder.countBefore(boundary));
                Assert.assertEquals("parquet countBefore(" + boundary + ')', expectedBefore, parquetFinder.countBefore(boundary));
                Assert.assertEquals("native countThrough(" + boundary + ')', expectedThrough, nativeFinder.countThrough(boundary));
                Assert.assertEquals("parquet countThrough(" + boundary + ')', expectedThrough, parquetFinder.countThrough(boundary));
            }
        }
    }

    private long countBefore(long[] timestamps, long boundary) {
        long count = 0;
        for (long timestamp : timestamps) {
            if (timestamp >= boundary) {
                break;
            }
            count++;
        }
        return count;
    }

    private long countThrough(long[] timestamps, long boundary) {
        long count = 0;
        for (long timestamp : timestamps) {
            if (timestamp > boundary) {
                break;
            }
            count++;
        }
        return count;
    }

    private void testFuzz(int duplicatesPerTick) throws Exception {
        final Rnd rnd = TestUtils.generateRandom(LOG);
        assertMemoryLeak(() -> {
            int timestampType = rnd.nextBoolean() ? ColumnType.TIMESTAMP_MICRO : ColumnType.TIMESTAMP_NANO;
            TimestampDriver driver = ColumnType.getTimestampDriver(timestampType);
            TableModel oracleModel = new TableModel(configuration, "oracle", PartitionBy.YEAR).timestamp(timestampType);
            AbstractCairoTest.create(oracleModel);
            TableModel model = new TableModel(configuration, "x", PartitionBy.YEAR).timestamp(timestampType);
            AbstractCairoTest.create(model);

            final long minTimestamp = driver.parseFloorLiteral("1980-01-01T00:00:00.000Z");
            long maxTimestamp = minTimestamp;
            long timestamp = minTimestamp;
            try (
                    TableWriter oracleWriter = newOffPoolWriter(configuration, "oracle");
                    TableWriter writer = newOffPoolWriter(configuration, "x")
            ) {
                int ticks = duplicatesPerTick;
                for (int i = 0; i < 1000; i++) {
                    oracleWriter.newRow(timestamp).append();
                    writer.newRow(timestamp).append();
                    maxTimestamp = timestamp;
                    if (--ticks == 0) {
                        if (duplicatesPerTick > 1) {
                            // we want to be in control of the number of duplicates
                            timestamp += driver.fromMinutes((int) (rnd.nextLong(1) + 1));
                        } else {
                            // extra duplicates are fine
                            timestamp += driver.fromMinutes((int) rnd.nextLong(2));
                        }
                        ticks = duplicatesPerTick;
                    }
                }

                // write one more row, so that the active partition contains it;
                // that's because we can't convert active partition to parquet
                long newerTimestamp = driver.parseFloorLiteral("2000-01-01T00:00:00.000Z");
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
                    ParquetPartitionDecoder partitionDecoder = new ParquetPartitionDecoder();
                    ParquetTimestampFinder finder = new ParquetTimestampFinder(partitionDecoder)
            ) {
                Assert.assertEquals(2, oracleReader.getPartitionCount());
                Assert.assertEquals(2, reader.getPartitionCount());

                oracleReader.openPartition(0);
                reader.openPartition(0);

                oracleFinder.of(oracleReader, 0, 0, 1000);
                finder.of(reader, 0, 0);

                // assert approx timestamps for both finders
                Assert.assertTrue(oracleFinder.minTimestampLowerBound() <= oracleFinder.maxTimestampUpperBound());
                Assert.assertTrue(finder.minTimestampLowerBound() <= finder.maxTimestampUpperBound());

                // prepare() must be called before accessing exact timestamps
                oracleFinder.prepare();
                finder.prepare();

                // assert approx vs. exact timestamps
                Assert.assertTrue(oracleFinder.minTimestampLowerBound() <= oracleFinder.minTimestampExact());
                Assert.assertTrue(finder.minTimestampLowerBound() <= finder.minTimestampExact());
                Assert.assertTrue(oracleFinder.maxTimestampUpperBound() >= oracleFinder.maxTimestampExact());
                Assert.assertTrue(finder.maxTimestampUpperBound() >= finder.maxTimestampExact());

                // assert exact timestamps
                Assert.assertEquals(minTimestamp, oracleFinder.minTimestampExact());
                Assert.assertEquals(oracleFinder.minTimestampExact(), finder.minTimestampExact());
                Assert.assertEquals(maxTimestamp, oracleFinder.maxTimestampExact());
                Assert.assertEquals(oracleFinder.maxTimestampExact(), finder.maxTimestampExact());

                for (long boundary : new long[]{
                        Long.MIN_VALUE,
                        minTimestamp,
                        maxTimestamp,
                        Long.MAX_VALUE
                }) {
                    Assert.assertEquals(oracleFinder.countBefore(boundary), finder.countBefore(boundary));
                    Assert.assertEquals(oracleFinder.countThrough(boundary), finder.countThrough(boundary));
                }
                Assert.assertEquals(0, finder.countBefore(Long.MIN_VALUE));
                Assert.assertEquals(0, finder.countThrough(Long.MIN_VALUE));
                Assert.assertEquals(1000, finder.countBefore(Long.MAX_VALUE));
                Assert.assertEquals(1000, finder.countThrough(Long.MAX_VALUE));

                for (int row = 0; row < 1000; row++) {
                    Assert.assertEquals(oracleFinder.timestampAt(row), finder.timestampAt(row));
                }

                final long start = System.nanoTime();
                long calls = 0;
                long minuteTimestamps = driver.fromMinutes(1);
                for (long ts = minTimestamp - minuteTimestamps; ts < maxTimestamp + minuteTimestamps; ts += minuteTimestamps) {
                    // full partition
                    Assert.assertEquals(
                            oracleFinder.findTimestamp(ts, 0, 1000 - 1),
                            finder.findTimestamp(ts, 0, 1000 - 1)
                    );

                    // first partition half
                    Assert.assertEquals(
                            oracleFinder.findTimestamp(ts, 0, 1000 / 2),
                            finder.findTimestamp(ts, 0, 1000 / 2)
                    );

                    // second partition half
                    Assert.assertEquals(
                            oracleFinder.findTimestamp(ts, 1000 / 2, 1000 - 1),
                            finder.findTimestamp(ts, 1000 / 2, 1000 - 1)
                    );

                    // partition middle
                    Assert.assertEquals(
                            oracleFinder.findTimestamp(ts, 1000 / 3, 2L * 1000 / 3),
                            finder.findTimestamp(ts, 1000 / 3, 2L * 1000 / 3)
                    );
                    Assert.assertEquals(oracleFinder.countBefore(ts), finder.countBefore(ts));
                    Assert.assertEquals(oracleFinder.countThrough(ts), finder.countThrough(ts));

                    calls += 12;
                }

                System.out.println("average call latency: " + ((System.nanoTime() - start) / calls) + "ns");
            }
        });
    }
}
