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

import io.questdb.PropertyKey;
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
import io.questdb.mp.WorkerPool;
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
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

/**
 * timestamp_floor_utc() with a constant named time zone caches the bucket of the last floored
 * timestamp. The cache must never change a result, so these tests compare the function with
 * the uncached arithmetic for timestamps in ascending, descending and random order, which hits,
 * misses and thrashes the cache. Runs that step back and forth over bucket boundaries then pin
 * the bounds of the cached range. timestamp_floor() runs through the same checks: it shares the
 * function classes and must stay uncached.
 */
public class TimestampFloorUtcBucketCachingFunctionTest extends AbstractCairoTest {
    private static final long MICROS_1962 = -252_460_800_000_000L;
    private static final long MICROS_2015 = 1_420_070_400_000_000L;
    private static final long MICROS_2021 = 1_609_459_200_000_000L;
    private static final long MICROS_2026 = 1_767_225_600_000_000L;
    private static final long MICROS_DAY = 86_400_000_000L;
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
            // not cached on a micro column: a bucket too narrow to hold several timestamps,
            // calendar units and nanosecond strides. The micro driver floors the latter in a
            // nanosecond domain, where a non-zero offset moves the buckets of a whole-micro
            // stride (7000n and 11_000n are 7 and 11 micros wide) off the grid that the cache's
            // fixed-width arithmetic assumes. A nano column caches all of them except the
            // calendar units and 5n, which is too narrow there as well.
            "1U", "1w", "1M", "1y", "5n", "500n", "7000n", "11_000n",
    };

    @Test
    public void testCacheHitsSkipMissPath() throws Exception {
        assertMemoryLeak(() -> {
            final FunctionFactory factory = new TimestampFloorFromOffsetUtcFunctionFactory();
            for (int timestampType : new int[]{ColumnType.TIMESTAMP_MICRO, ColumnType.TIMESTAMP_NANO}) {
                final TimestampDriver driver = ColumnType.getTimestampDriver(timestampType);
                for (char unit : new char[]{'h', 'd'}) {
                    // The offset selects the named-zone implementation with DST gap correction.
                    for (boolean hasOffset : new boolean[]{false, true}) {
                        final String stride = "1" + unit;
                        final String offset = hasOffset ? "00:15" : "00:00";
                        final String message = ColumnType.nameOf(timestampType) + ", " + stride + ", " + offset;
                        final TimestampHolder holder = new TimestampHolder(timestampType);
                        try (Function func = newFloorFunction(factory, holder, stride, Numbers.LONG_NULL, offset, "Europe/Berlin")) {
                            Assert.assertFalse(message, func.isThreadSafe());
                            // Berlin is UTC+1 here: 2015-01-01 00:30 UTC floors to 00:00 for
                            // an hour and the previous day's 23:00 for a day, plus the offset.
                            final long midnight = driver.from(MICROS_2015, ColumnType.TIMESTAMP_MICRO);
                            final long expected = midnight - (unit == 'd' ? driver.fromHours(1) : 0)
                                    + (hasOffset ? driver.fromMinutes(15) : 0);
                            holder.value = midnight + driver.fromMinutes(30);
                            Assert.assertEquals(message, expected, func.getTimestamp(null));
                            holder.value++;
                            Assert.assertEquals(message, expected, func.getTimestamp(null));
                            final String state = stateOf(func);
                            holder.value++;
                            Assert.assertEquals(message, expected, func.getTimestamp(null));
                            // A hit leaves all fields unchanged; a miss updates lastMissTimestamp.
                            Assert.assertEquals(message, state, stateOf(func));
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testCachedFloorMatchesUncachedMicros() throws Exception {
        assertMemoryLeak(() -> assertCachedFloorMatchesUncached(ColumnType.TIMESTAMP_MICRO));
    }

    @Test
    public void testCachedFloorMatchesUncachedNanos() throws Exception {
        assertMemoryLeak(() -> assertCachedFloorMatchesUncached(ColumnType.TIMESTAMP_NANO));
    }

    @Test
    public void testDayBucketBounds() throws Exception {
        // In summer, a day bucket of Europe/Berlin starts at 22:00 UTC. The first two timestamps
        // make the function cache the bucket that ends there, so the cache must not serve the
        // third one, the start of the next bucket. The third one makes the function cache that
        // next bucket, so the cache must not serve the fourth one, the microsecond before it.
        assertMemoryLeak(() -> {
            final TimestampHolder holder = new TimestampHolder(ColumnType.TIMESTAMP_MICRO);
            final Function func = newFloorFunction(new TimestampFloorFromOffsetUtcFunctionFactory(), holder, "1d", Numbers.LONG_NULL, "00:00", "Europe/Berlin");
            try {
                assertFloor("2021-06-14T22:00:00.000000Z", func, holder, "2021-06-15T20:00:00.000000Z");
                assertFloor("2021-06-14T22:00:00.000000Z", func, holder, "2021-06-15T21:00:00.000000Z");
                assertFloor("2021-06-15T22:00:00.000000Z", func, holder, "2021-06-15T22:00:00.000000Z");
                assertFloor("2021-06-14T22:00:00.000000Z", func, holder, "2021-06-15T21:59:59.999999Z");
            } finally {
                Misc.free(func);
            }
        });
    }

    @Test
    public void testNullTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            final String[] zones = {null, "+05:30", "Europe/Berlin"};
            // The named time zone builds one of two function classes: an origin that is not
            // day-aligned, such as offset 00:15, selects the DST gap aware one.
            final String[] offsets = {"00:00", "00:15"};
            final FunctionFactory[] factories = {new TimestampFloorFromOffsetUtcFunctionFactory(), new TimestampFloorFromOffsetFunctionFactory()};
            for (FunctionFactory factory : factories) {
                for (String zone : zones) {
                    for (String offset : offsets) {
                        final TimestampHolder holder = new TimestampHolder(ColumnType.TIMESTAMP_MICRO);
                        final Function func = newFloorFunction(factory, holder, "1d", Numbers.LONG_NULL, offset, zone);
                        holder.value = Numbers.LONG_NULL;
                        Assert.assertEquals(Numbers.LONG_NULL, func.getTimestamp(null));
                        // A caching function stores a bucket on the second of two misses that are
                        // close to each other. Check that the stored bucket does not serve NULL.
                        holder.value = MICROS_2015 + 12_345;
                        Assert.assertNotEquals(Numbers.LONG_NULL, func.getTimestamp(null));
                        holder.value = MICROS_2015 + 12_346;
                        Assert.assertNotEquals(Numbers.LONG_NULL, func.getTimestamp(null));
                        holder.value = Numbers.LONG_NULL;
                        Assert.assertEquals(Numbers.LONG_NULL, func.getTimestamp(null));
                        Misc.free(func);
                    }
                }
            }
        });
    }

    @Test
    public void testParallelSampleByWithNamedTimeZone() throws Exception {
        // Workers must floor through their own clones of the caching function. With a function
        // shared by workers that scan different page frames, one worker reads the cached bucket
        // while another worker replaces it, which pairs the range of one bucket with the result
        // of another and moves rows to wrong buckets. The table is large and the page frames
        // are small, so that several workers floor timestamps of different buckets at the same
        // time. The table holds one row per second, 1,000 hours starting at MICROS_2026.
        assertParallelSampleBy(
                """
                        CREATE TABLE x AS (
                            SELECT timestamp_sequence('2026-01-01', 1_000_000) ts
                            FROM long_sequence(3_600_000)
                        ) TIMESTAMP(ts) PARTITION BY DAY
                        """,
                "1h",
                Micros.HOUR_MICROS
        );
    }

    @Test
    public void testThreadSafeFunctionKeepsNoState() throws Exception {
        // Workers share a function that reports thread safety, so its calls must not change any
        // of its fields. A function that caches buckets changes them and must report the
        // opposite. The epoch and a repeated timestamp go first: these are the calls most
        // likely to make a function store a bucket.
        assertMemoryLeak(() -> {
            final String[] strides = {"1U", "7U", "8U", "1n", "7n", "8n", "500n", "11_000n", "1s", "1h", "1d", "1w", "1M", "1y"};
            final long[] timestamps = {0, 0, 1, MICROS_2015 + 12_345, MICROS_2015 + 12_346, Numbers.LONG_NULL};
            final int[] timestampTypes = {ColumnType.TIMESTAMP_MICRO, ColumnType.TIMESTAMP_NANO};
            final FunctionFactory[] factories = {new TimestampFloorFromOffsetUtcFunctionFactory(), new TimestampFloorFromOffsetFunctionFactory()};
            for (FunctionFactory factory : factories) {
                for (int timestampType : timestampTypes) {
                    for (String stride : strides) {
                        final TimestampHolder holder = new TimestampHolder(timestampType);
                        final Function func = newFloorFunction(factory, holder, stride, Numbers.LONG_NULL, "00:00", "Europe/Berlin");
                        try {
                            final String state = stateOf(func);
                            for (long timestamp : timestamps) {
                                holder.value = timestamp;
                                func.getTimestamp(null);
                            }
                            Assert.assertEquals(
                                    factory.getSignature() + ", " + ColumnType.nameOf(timestampType) + ", " + stride,
                                    func.isThreadSafe(),
                                    state.equals(stateOf(func))
                            );
                        } finally {
                            Misc.free(func);
                        }
                    }
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

            // A function stores a bucket on the second of two misses that are closer to each
            // other than 1/8 of the bucket width. A bucket narrower than 8 units of the timestamp
            // resolution rounds that distance down to zero, so a cache has nothing to gain and the
            // function stays uncached. A bucket of 8 units is the narrowest cached one.
            assertThreadSafety(true, utcFactory, ColumnType.TIMESTAMP_MICRO, "1U", "Europe/Berlin");
            assertThreadSafety(true, utcFactory, ColumnType.TIMESTAMP_MICRO, "7U", "Europe/Berlin");
            assertThreadSafety(false, utcFactory, ColumnType.TIMESTAMP_MICRO, "8U", "Europe/Berlin");
            assertThreadSafety(true, utcFactory, ColumnType.TIMESTAMP_NANO, "1n", "Europe/Berlin");
            assertThreadSafety(true, utcFactory, ColumnType.TIMESTAMP_NANO, "7n", "Europe/Berlin");
            assertThreadSafety(false, utcFactory, ColumnType.TIMESTAMP_NANO, "8n", "Europe/Berlin");

            // calendar units and nanosecond strides on a micro column are not cached, whether the
            // stride is below the micro resolution or a whole number of micros
            assertThreadSafety(true, utcFactory, ColumnType.TIMESTAMP_MICRO, "1w", "Europe/Berlin");
            assertThreadSafety(true, utcFactory, ColumnType.TIMESTAMP_MICRO, "1M", "Europe/Berlin");
            assertThreadSafety(true, utcFactory, ColumnType.TIMESTAMP_NANO, "1M", "Europe/Berlin");
            assertThreadSafety(true, utcFactory, ColumnType.TIMESTAMP_MICRO, "1y", "Europe/Berlin");
            assertThreadSafety(true, utcFactory, ColumnType.TIMESTAMP_MICRO, "500n", "Europe/Berlin");
            assertThreadSafety(true, utcFactory, ColumnType.TIMESTAMP_MICRO, "1000n", "Europe/Berlin");
            assertThreadSafety(true, utcFactory, ColumnType.TIMESTAMP_MICRO, "11_000n", "Europe/Berlin");
            // no time zone, UTC and fixed-offset time zones are not cached
            assertThreadSafety(true, utcFactory, ColumnType.TIMESTAMP_MICRO, "1h", null);
            assertThreadSafety(true, utcFactory, ColumnType.TIMESTAMP_MICRO, "1d", "UTC");
            assertThreadSafety(true, utcFactory, ColumnType.TIMESTAMP_MICRO, "1d", "+05:30");
            // the return-local mode re-floors timestamps in DST gaps and stays uncached
            assertThreadSafety(true, localFactory, ColumnType.TIMESTAMP_MICRO, "1h", "Europe/Berlin");
            assertThreadSafety(true, localFactory, ColumnType.TIMESTAMP_NANO, "1h", "Europe/Berlin");
            assertThreadSafety(true, localFactory, ColumnType.TIMESTAMP_MICRO, "1d", "Europe/Berlin");
        });
    }

    private static void assertFloor(String expected, Function func, TimestampHolder holder, String timestamp) throws NumericException {
        holder.value = MicrosFormatUtils.parseUTCTimestamp(timestamp);
        final StringSink actual = new StringSink();
        MicrosFormatUtils.appendDateTimeUSec(actual, func.getTimestamp(null));
        TestUtils.assertEquals("floor of " + timestamp, expected, actual);
    }

    private static void assertThreadSafety(boolean expected, FunctionFactory factory, int timestampType, String stride, String zone) throws SqlException {
        for (boolean isArgumentThreadSafe : new boolean[]{true, false}) {
            // The origins exercise both constant named-zone branches, with and without DST gap correction.
            for (Object[] origin : ORIGINS) {
                final TimestampHolder holder = new TimestampHolder(timestampType) {
                    @Override
                    public boolean isThreadSafe() {
                        return isArgumentThreadSafe;
                    }
                };
                try (Function func = newFloorFunction(factory, holder, stride, (long) origin[2], (String) origin[0], zone)) {
                    Assert.assertEquals(
                            factory.getSignature() + ", " + ColumnType.nameOf(timestampType) + ", " + stride + ", " + zone
                                    + ", " + origin[0] + ", " + origin[2] + ", argument thread-safe=" + isArgumentThreadSafe,
                            expected && isArgumentThreadSafe,
                            func.isThreadSafe()
                    );
                }
            }
        }
    }

    private static TimeZoneRules getZoneRules(TimestampDriver driver, String zone) throws NumericException {
        return DateLocaleFactory.EN_LOCALE.getZoneRules(
                Numbers.decodeLowInt(DateLocaleFactory.EN_LOCALE.matchZone(zone, 0, zone.length())),
                driver.getTZRuleResolution()
        );
    }

    // Runs of timestamps around bucket boundaries, to be floored in this order, which is not
    // monotonic. The uncached arithmetic provides the boundaries as UTC instants: the start of
    // the bucket a day past each window center and the two bucket starts before it, so that
    // day buckets get boundaries on both sides of a transition. A function caches a bucket on
    // the second of two misses that are closer to each other than 1/8 of the bucket width. So
    // the first three timestamps of a run make a caching function store the bucket before the
    // boundary and then the one that starts at it. The rest alternates between bucketStart - 1
    // and bucketStart: each of the two arrives while the cache holds the other one's bucket
    // and must miss, and arrives again while the cache holds its own bucket and must hit.
    private static LongList newBoundaryRuns(
            TimestampDriver driver,
            LongList centers,
            TimestampDriver.TimestampFloorWithOffsetMethod floor,
            int stride,
            char unit,
            long effectiveOffset,
            TimeZoneRules rules
    ) {
        final LongList list = new LongList();
        for (int c = 0, n = centers.size(); c < n; c++) {
            long bucketStart = uncachedFloor(floor, centers.getQuick(c) + driver.fromDays(1), stride, unit, effectiveOffset, rules, true);
            for (int i = 0; i < 3; i++) {
                // Timestamps below a non-epoch origin floor to the origin, which leaves no
                // bucket before the boundary. The step is then the smallest one.
                final long prevBucketStart = uncachedFloor(floor, bucketStart - 1, stride, unit, effectiveOffset, rules, true);
                final long step = Math.max(1, (bucketStart - prevBucketStart) / 32);
                list.add(bucketStart - 3 * step);
                list.add(bucketStart - 2 * step);
                list.add(bucketStart + step);
                list.add(bucketStart - 1);
                list.add(bucketStart - 1);
                list.add(bucketStart);
                list.add(bucketStart);
                list.add(bucketStart - 1);
                bucketStart = prevBucketStart;
            }
        }
        return list;
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

    // Timestamps around every window center plus a sparse sweep of 1880-2100, sorted.
    // A function caches a bucket only after two misses that are close to each other relative to
    // the bucket width, so each center also gets bursts dense enough for the narrow buckets.
    private static long[] newTimestamps(TimestampDriver driver, LongList centers) {
        final LongList list = new LongList();
        final long windowStep = driver.fromMinutes(23) + 17;
        final long window = driver.fromDays(2) + driver.fromHours(12);
        final long[] burstSteps = {
                driver.from(7_000_003, ColumnType.TIMESTAMP_MICRO),
                driver.from(100_001, ColumnType.TIMESTAMP_MICRO),
                driver.from(20_001, ColumnType.TIMESTAMP_MICRO),
        };
        for (int c = 0, n = centers.size(); c < n; c++) {
            final long center = centers.getQuick(c);
            // the instants right at the center
            list.add(center - 1);
            list.add(center);
            for (long ts = center - window; ts < center + window; ts += windowStep) {
                list.add(ts);
            }
            for (long burstStep : burstSteps) {
                for (int i = -100; i < 100; i++) {
                    list.add(center + i * burstStep);
                }
            }
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

    // The centers of the dense timestamp windows: every tz transition in 2015-2026 plus two
    // instants that do not depend on the time zone. The latter make the functions cache
    // buckets before 1970 and in the zones that have no transition in 2015-2026.
    private static LongList newWindowCenters(TimestampDriver driver, TimeZoneRules rules) {
        final LongList centers = new LongList();
        centers.add(driver.from(MICROS_1962, ColumnType.TIMESTAMP_MICRO));
        centers.add(driver.from(MICROS_2021, ColumnType.TIMESTAMP_MICRO));
        final long hi = driver.from(MICROS_2026, ColumnType.TIMESTAMP_MICRO);
        long transition = rules.getNextDST(driver.from(MICROS_2015, ColumnType.TIMESTAMP_MICRO));
        while (transition < hi) {
            centers.add(transition);
            transition = rules.getNextDST(transition);
        }
        return centers;
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

    // The values of the function's primitive instance fields and the identities of the objects
    // that its other instance fields refer to, the inherited ones included. The state of those
    // objects is out of reach.
    private static String stateOf(Function func) throws IllegalAccessException {
        final StringSink sink = new StringSink();
        for (Class<?> c = func.getClass(); c != Object.class; c = c.getSuperclass()) {
            for (Field field : c.getDeclaredFields()) {
                if (!Modifier.isStatic(field.getModifiers())) {
                    field.setAccessible(true);
                    final Object value = field.get(func);
                    sink.put(field.getName()).put('=');
                    if (field.getType().isPrimitive()) {
                        sink.put(String.valueOf(value));
                    } else {
                        sink.put(System.identityHashCode(value));
                    }
                    sink.put(';');
                }
            }
        }
        return sink.toString();
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
            final LongList centers = newWindowCenters(driver, rules);
            assertCachedFloorMatchesUncached(driver, zone, rules, centers, newTimestamps(driver, centers));
        }
    }

    private void assertCachedFloorMatchesUncached(
            TimestampDriver driver,
            String zone,
            TimeZoneRules rules,
            LongList centers,
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
                    final LongList boundaryRuns = newBoundaryRuns(driver, centers, floor, strideMultiple, unit, effectiveOffset, rules);
                    final TimestampHolder holder = new TimestampHolder(driver.getTimestampType());
                    final Function func = newFloorFunction(factories[f], holder, stride, fromMicros, (String) origin[0], zone);
                    try {
                        for (int pass = 0; pass < 4; pass++) {
                            for (int i = 0, n = pass < 3 ? timestamps.length : boundaryRuns.size(); i < n; i++) {
                                final long timestamp = switch (pass) {
                                    case 0 -> timestamps[i];
                                    case 1 -> timestamps[n - i - 1];
                                    case 2 -> shuffled[i];
                                    default -> boundaryRuns.getQuick(i);
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

    // Runs SAMPLE BY with the stride and a named time zone on 4 workers over small page frames.
    // The table must hold 3,600 rows for each of 1,000 buckets, the first one at MICROS_2026.
    private void assertParallelSampleBy(String ddl, String stride, long bucketWidth) throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 10_000);
        assertMemoryLeak(() -> {
            final WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)));
            TestUtils.execute(
                    pool,
                    (engine, _, sqlExecutionContext) -> {
                        engine.execute(ddl, sqlExecutionContext);

                        final StringSink expected = new StringSink();
                        expected.put("ts\tcount\n");
                        for (int i = 0; i < 1_000; i++) {
                            MicrosFormatUtils.appendDateTimeUSec(expected, MICROS_2026 + i * bucketWidth);
                            expected.put("\t3600\n");
                        }

                        assertQuery("SELECT ts, count() FROM x SAMPLE BY " + stride + " ALIGN TO CALENDAR TIME ZONE 'Europe/Berlin'")
                                .withEngine(engine)
                                .withContext(sqlExecutionContext)
                                .noLeakCheck()
                                .timestamp("ts")
                                .expectSize()
                                .withPlan("""
                                        Encode sort light
                                          keys: [ts]
                                            Async Group By workers: 4
                                              keys: [ts]
                                              keyFunctions: [timestamp_floor_utc('%s',ts,null,'00:00','Europe/Berlin')]
                                              values: [count(*)]
                                              filter: null
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: x
                                        """.formatted(stride))
                                .returns(expected);
                    },
                    configuration,
                    LOG
            );
        });
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
