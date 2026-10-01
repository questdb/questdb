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

package io.questdb.test.griffin.engine.functions.date;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.TimestampFunction;
import io.questdb.griffin.engine.functions.constants.StrConstant;
import io.questdb.griffin.engine.functions.constants.TimestampConstant;
import io.questdb.griffin.engine.functions.date.TimestampFloorFromOffsetFunctionFactory;
import io.questdb.griffin.engine.functions.date.TimestampFloorFromOffsetUtcFunctionFactory;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.CommonUtils;
import io.questdb.std.datetime.DateLocaleFactory;
import io.questdb.std.datetime.TimeZoneRules;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * timestamp_floor_utc() with a constant named time zone caches the bucket of the last floored
 * timestamp. The cache must never change a result, so these tests compare the function with
 * the uncached arithmetic for timestamps in ascending, descending and random order, which hits,
 * misses and thrashes the cache. timestamp_floor() runs through the same checks: it shares the
 * function classes and must stay uncached.
 */
public class TimestampFloorUtcBucketCachingFunctionTest extends AbstractCairoTest {
    private static final long MICROS_2015 = 1_420_070_400_000_000L;
    private static final long MICROS_2026 = 1_767_225_600_000_000L;
    private static final long MICROS_DAY = 86_400_000_000L;
    private static final long MICROS_MINUTE = 60_000_000L;
    private static final String[] NAMED_ZONES = {
            "Africa/Casablanca",
            "America/Havana",
            "America/New_York",
            "America/Sao_Paulo",
            "America/St_Johns",
            "Antarctica/Troll",
            "Asia/Beirut",
            "Asia/Kathmandu",
            "Asia/Kolkata",
            "Asia/Tehran",
            "Australia/Lord_Howe",
            "Europe/Berlin",
            "Europe/Dublin",
            "Pacific/Apia",
            "Pacific/Chatham",
    };
    // offset string, offset in minutes, FROM in micros or LONG_NULL
    private static final Object[][] ORIGINS = {
            {"00:00", 0, Numbers.LONG_NULL},
            {"00:15", 15, Numbers.LONG_NULL},
            // 2019-07-01T07:13:00Z, neither day nor hour aligned
            {"01:00", 60, 1_561_965_180_000_000L},
    };
    private static final String[] STRIDES = {
            "250T", "1s", "5s", "1m", "15m", "17m", "1h", "7h", "1d", "3d",
            // never cached: a bucket too narrow to hold several timestamps, calendar units and
            // a stride below the micro resolution
            "1U", "1w", "1M", "1y", "500n",
    };

    @Test
    public void testCachedFloorMatchesUncachedMicros() throws Exception {
        assertMemoryLeak(() -> assertCachedFloorMatchesUncached(ColumnType.TIMESTAMP_MICRO));
    }

    @Test
    public void testCachedFloorMatchesUncachedNanos() throws Exception {
        assertMemoryLeak(() -> assertCachedFloorMatchesUncached(ColumnType.TIMESTAMP_NANO));
    }

    @Test
    public void testNullTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            final String[] zones = {null, "+05:30", "Europe/Berlin"};
            final FunctionFactory[] factories = {new TimestampFloorFromOffsetUtcFunctionFactory(), new TimestampFloorFromOffsetFunctionFactory()};
            for (FunctionFactory factory : factories) {
                for (String zone : zones) {
                    final TimestampHolder holder = new TimestampHolder(ColumnType.TIMESTAMP_MICRO);
                    final Function func = newFloorFunction(factory, holder, "1d", Numbers.LONG_NULL, "00:00", zone);
                    holder.value = Numbers.LONG_NULL;
                    Assert.assertEquals(Numbers.LONG_NULL, func.getTimestamp(null));
                    // warm up the cache, then check that it does not serve NULL
                    holder.value = MICROS_2015 + 12_345;
                    Assert.assertNotEquals(Numbers.LONG_NULL, func.getTimestamp(null));
                    holder.value = Numbers.LONG_NULL;
                    Assert.assertEquals(Numbers.LONG_NULL, func.getTimestamp(null));
                    Misc.free(func);
                }
            }
        });
    }

    @Test
    public void testThreadSafety() throws Exception {
        // Functions that cache buckets hold mutable state, so parallel execution must clone them
        // per worker. The other shapes stay as thread-safe as their timestamp argument.
        assertMemoryLeak(() -> {
            final FunctionFactory utcFactory = new TimestampFloorFromOffsetUtcFunctionFactory();
            final FunctionFactory localFactory = new TimestampFloorFromOffsetFunctionFactory();

            assertThreadSafety(false, utcFactory, ColumnType.TIMESTAMP_MICRO, "1s", "Europe/Berlin");
            assertThreadSafety(false, utcFactory, ColumnType.TIMESTAMP_MICRO, "1h", "Europe/Berlin");
            assertThreadSafety(false, utcFactory, ColumnType.TIMESTAMP_MICRO, "1d", "Europe/Berlin");
            assertThreadSafety(false, utcFactory, ColumnType.TIMESTAMP_NANO, "500n", "Europe/Berlin");

            // calendar units and a stride below the micro resolution are not cached
            assertThreadSafety(true, utcFactory, ColumnType.TIMESTAMP_MICRO, "1w", "Europe/Berlin");
            assertThreadSafety(true, utcFactory, ColumnType.TIMESTAMP_MICRO, "1M", "Europe/Berlin");
            assertThreadSafety(true, utcFactory, ColumnType.TIMESTAMP_MICRO, "1y", "Europe/Berlin");
            assertThreadSafety(true, utcFactory, ColumnType.TIMESTAMP_MICRO, "500n", "Europe/Berlin");
            // no time zone, UTC and fixed-offset time zones are not cached
            assertThreadSafety(true, utcFactory, ColumnType.TIMESTAMP_MICRO, "1h", null);
            assertThreadSafety(true, utcFactory, ColumnType.TIMESTAMP_MICRO, "1d", "UTC");
            assertThreadSafety(true, utcFactory, ColumnType.TIMESTAMP_MICRO, "1d", "+05:30");
            // the return-local mode re-floors timestamps in DST gaps and stays uncached
            assertThreadSafety(true, localFactory, ColumnType.TIMESTAMP_MICRO, "1h", "Europe/Berlin");
            assertThreadSafety(true, localFactory, ColumnType.TIMESTAMP_MICRO, "1d", "Europe/Berlin");
        });
    }

    private static void assertThreadSafety(boolean expected, FunctionFactory factory, int timestampType, String stride, String zone) throws SqlException {
        final Function func = newFloorFunction(factory, new TimestampHolder(timestampType), stride, Numbers.LONG_NULL, "00:00", zone);
        Assert.assertEquals(stride + ", " + zone, expected, func.isThreadSafe());
        Misc.free(func);
    }

    private static TimeZoneRules getZoneRules(TimestampDriver driver, String zone) throws NumericException {
        return DateLocaleFactory.EN_LOCALE.getZoneRules(
                Numbers.decodeLowInt(DateLocaleFactory.EN_LOCALE.matchZone(zone, 0, zone.length())),
                driver.getTZRuleResolution()
        );
    }

    private static Function newFloorFunction(
            FunctionFactory factory,
            TimestampHolder holder,
            String stride,
            long fromMicros,
            String offset,
            String zone
    ) throws SqlException {
        final ObjList<Function> args = new ObjList<>();
        args.add(new StrConstant(stride));
        args.add(holder);
        args.add(TimestampConstant.newInstance(fromMicros, ColumnType.TIMESTAMP_MICRO));
        args.add(new StrConstant(offset));
        args.add(zone != null ? new StrConstant(zone) : StrConstant.NULL);
        final IntList argPositions = new IntList();
        argPositions.setAll(args.size(), 0);
        return factory.newInstance(0, args, argPositions, configuration, sqlExecutionContext);
    }

    // Timestamps around every tz transition in 2015-2026 plus a sparse sweep of 1880-2100, sorted.
    // A function caches a bucket only after two misses that are close to each other relative to
    // the bucket width, so each transition also gets bursts dense enough for the narrow buckets.
    private static long[] newTimestamps(TimestampDriver driver, TimeZoneRules rules) {
        final LongList list = new LongList();
        final long windowStep = driver.fromMinutes(23) + 17;
        final long window = driver.fromDays(2) + driver.fromHours(12);
        final long[] burstSteps = {
                driver.from(7_000_003, ColumnType.TIMESTAMP_MICRO),
                driver.from(100_001, ColumnType.TIMESTAMP_MICRO),
                driver.from(20_001, ColumnType.TIMESTAMP_MICRO),
        };
        final long hi = driver.from(MICROS_2026, ColumnType.TIMESTAMP_MICRO);
        long transition = rules.getNextDST(driver.from(MICROS_2015, ColumnType.TIMESTAMP_MICRO));
        while (transition < hi) {
            // the instants right at the transition
            list.add(transition - 1);
            list.add(transition);
            for (long ts = transition - window; ts < transition + window; ts += windowStep) {
                list.add(ts);
            }
            for (long burstStep : burstSteps) {
                for (int i = -100; i < 100; i++) {
                    list.add(transition + i * burstStep);
                }
            }
            transition = rules.getNextDST(transition);
        }
        final long sweepStep = driver.fromDays(11) + driver.fromMinutes(137) + 31;
        final long sweepHi = driver.from(130 * 365 * MICROS_DAY, ColumnType.TIMESTAMP_MICRO);
        for (long ts = driver.from(-90 * 365 * MICROS_DAY, ColumnType.TIMESTAMP_MICRO); ts < sweepHi; ts += sweepStep) {
            list.add(ts);
        }
        list.sort();
        final long[] timestamps = new long[list.size()];
        for (int i = 0; i < timestamps.length; i++) {
            timestamps[i] = list.getQuick(i);
        }
        return timestamps;
    }

    private static long[] shuffle(long[] timestamps) {
        final long[] shuffled = timestamps.clone();
        final Rnd rnd = new Rnd();
        for (int i = shuffled.length - 1; i > 0; i--) {
            final int j = rnd.nextInt(i + 1);
            final long tmp = shuffled[i];
            shuffled[i] = shuffled[j];
            shuffled[j] = tmp;
        }
        return shuffled;
    }

    // the uncached arithmetic of the floor functions
    private static long uncachedFloor(
            TimestampDriver.TimestampFloorWithOffsetMethod floor,
            long timestamp,
            int stride,
            char unit,
            long effectiveOffset,
            TimeZoneRules rules,
            boolean returnUtc
    ) {
        if (returnUtc) {
            final long tzOff = CommonUtils.getFloorUtcTzOffset(rules, timestamp, unit);
            final long floored = floor.floor(timestamp + tzOff, stride, effectiveOffset);
            return CommonUtils.offsetFlooredUtcResult(floored, tzOff, 0, rules, unit);
        }
        long floored = floor.floor(timestamp + rules.getOffset(timestamp), stride, effectiveOffset);
        final long gapDuration = rules.getDstGapOffset(floored);
        if (gapDuration != 0) {
            floored = floor.floor(floored - gapDuration, stride, effectiveOffset);
        }
        return floored;
    }

    private void assertCachedFloorMatchesUncached(int timestampType) throws Exception {
        final TimestampDriver driver = ColumnType.getTimestampDriver(timestampType);
        for (String zone : NAMED_ZONES) {
            final TimeZoneRules rules = getZoneRules(driver, zone);
            Assert.assertFalse(zone, rules.hasFixedOffset());
            assertCachedFloorMatchesUncached(driver, zone, rules, newTimestamps(driver, rules));
        }
    }

    private void assertCachedFloorMatchesUncached(
            TimestampDriver driver,
            String zone,
            TimeZoneRules rules,
            long[] timestamps
    ) throws SqlException {
        final long[] shuffled = shuffle(timestamps);
        final FunctionFactory[] factories = {new TimestampFloorFromOffsetUtcFunctionFactory(), new TimestampFloorFromOffsetFunctionFactory()};
        for (int f = 0; f < factories.length; f++) {
            final boolean returnUtc = f == 0;
            for (String stride : STRIDES) {
                final int strideMultiple = CommonUtils.getStrideMultiple(stride, 0);
                final char unit = CommonUtils.getStrideUnit(stride, 0);
                final TimestampDriver.TimestampFloorWithOffsetMethod floor = driver.getTimestampFloorWithOffsetMethod(unit);
                for (Object[] origin : ORIGINS) {
                    final long fromMicros = (long) origin[2];
                    final long from = fromMicros != Numbers.LONG_NULL ? driver.from(fromMicros, ColumnType.TIMESTAMP_MICRO) : 0;
                    final long effectiveOffset = from + driver.fromMinutes((int) origin[1]);
                    final TimestampHolder holder = new TimestampHolder(driver.getTimestampType());
                    final Function func = newFloorFunction(factories[f], holder, stride, fromMicros, (String) origin[0], zone);
                    try {
                        for (int pass = 0; pass < 3; pass++) {
                            for (int i = 0, n = timestamps.length; i < n; i++) {
                                final long timestamp = switch (pass) {
                                    case 0 -> timestamps[i];
                                    case 1 -> timestamps[n - i - 1];
                                    default -> shuffled[i];
                                };
                                holder.value = timestamp;
                                final long expected = uncachedFloor(floor, timestamp, strideMultiple, unit, effectiveOffset, rules, returnUtc);
                                final long actual = func.getTimestamp(null);
                                if (expected != actual) {
                                    Assert.fail("floor mismatch [zone=" + zone
                                            + ", stride=" + stride
                                            + ", offset=" + origin[0]
                                            + ", from=" + fromMicros
                                            + ", returnUtc=" + returnUtc
                                            + ", pass=" + pass
                                            + ", timestamp=" + timestamp
                                            + ", expected=" + expected
                                            + ", actual=" + actual
                                            + ']');
                                }
                            }
                        }
                    } finally {
                        Misc.free(func);
                    }
                }
            }
        }
    }

    private static class TimestampHolder extends TimestampFunction {
        private long value;

        private TimestampHolder(int timestampType) {
            super(timestampType);
        }

        @Override
        public long getTimestamp(Record rec) {
            return value;
        }

        @Override
        public boolean isThreadSafe() {
            return true;
        }
    }
}
