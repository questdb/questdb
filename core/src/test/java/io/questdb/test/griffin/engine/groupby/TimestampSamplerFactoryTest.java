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

package io.questdb.test.griffin.engine.groupby;

import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.groupby.TimestampSampler;
import io.questdb.griffin.engine.groupby.TimestampSamplerFactory;
import io.questdb.std.NumericException;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class TimestampSamplerFactoryTest {
    private static final long[] micros = {1, Timestamps.SECOND_MICROS, Timestamps.MINUTE_MICROS, Timestamps.HOUR_MICROS, Timestamps.DAY_MICROS};
    private static final char[] units = {'U', 's', 'm', 'h', 'd'};

    @Test
    public void testBucketIndex() throws SqlException {
        final Rnd rand = new Rnd();
        final StringSink sink = new StringSink();
        for (int i = 0; i < 1000; i++) {
            sink.clear();
            final int unitIndex = rand.nextInt(units.length);
            final char unit = units[unitIndex];
            final int amount = rand.nextInt(1000) + 1;
            sink.put(amount);
            sink.put(unit);
            final TimestampSampler sampler = TimestampSamplerFactory.getInstance(sink, 0);
            long startTimestamp = 0;
            long currentTimestamp = startTimestamp;
            sampler.setStart(startTimestamp);
            for (int j = 0; j < 100; j++) {
                currentTimestamp = sampler.nextTimestamp(currentTimestamp);
                Assert.assertEquals(j + 1, sampler.bucketIndex(currentTimestamp));
            }
        }
    }

    @Test
    public void testFindIntervalEndIndex() throws SqlException {
        assertFindIntervalEndIndexFailure(1, "missing interval", null, 1);
        assertFindIntervalEndIndexFailure(1_002, "expected interval qualifier", "45", 1000);
        assertFindIntervalEndIndexFailure(42, "expected interval qualifier", "", 42);
        assertFindIntervalEndIndexFailure(50, "expected single letter qualifier", "1bar", 49);
        assertFindIntervalEndIndexFailure(100, "negative interval is not allowed", "-", 100);

        Assert.assertEquals(0, TimestampSamplerFactory.findIntervalEndIndex("m", 11));
    }

    @Test
    public void testLongQualifier() {
        try {
            TimestampSamplerFactory.getInstance("1sa", 130);
            Assert.fail();
        } catch (SqlException e) {
            Assert.assertEquals(131, e.getPosition());
            TestUtils.assertContains(e.getFlyweightMessage(), "expected single letter qualifier");
        }
    }

    @Test
    public void testMicros() throws NumericException, SqlException {
        final StringSink sink = new StringSink();
        final long ts = TimestampFormatUtils.parseUTCTimestamp("2022-04-23T10:33:00.123456Z");
        final Rnd rand = new Rnd();
        for (int j = 0; j < 1000; j++) {
            final int k = rand.nextInt(1000001);
            final TimestampSampler sampler = createTimestampSampler(k, 'U', sink);
            final long bucketSize = (k == 0 ? 1 : k);
            final long expectedTs = ts - ts % bucketSize;
            for (int i = 0; i < bucketSize; i += 4) {
                long actualTs = sampler.round(expectedTs + i);
                if (expectedTs != actualTs) {
                    Assert.fail(String.format(
                            "Failed at: %s, i: %d. Expected: %s, actual: %s",
                            sink, i, Timestamps.toString(expectedTs), Timestamps.toString(actualTs))
                    );
                }
            }
        }
    }

    @Test
    public void testMillis() throws NumericException, SqlException {
        final StringSink sink = new StringSink();
        final long ts = TimestampFormatUtils.parseUTCTimestamp("2022-04-23T10:33:00.123456Z");
        for (int k = 0; k < 1001; k++) {
            final TimestampSampler sampler = createTimestampSampler(k, 'T', sink);
            final long bucketSize = Timestamps.MILLI_MICROS * (k == 0 ? 1 : k);
            final long expectedTs = ts - ts % bucketSize;
            for (int i = 0; i < bucketSize; i += 40) {
                long actualTs = sampler.round(expectedTs + i);
                if (expectedTs != actualTs) {
                    Assert.fail(String.format(
                            "Failed at: %s, i: %d. Expected: %s, actual: %s",
                            sink, i, Timestamps.toString(expectedTs), Timestamps.toString(actualTs))
                    );
                }
            }
        }
    }

    @Test
    public void testMinutes() throws NumericException, SqlException {
        final StringSink sink = new StringSink();
        final long ts = TimestampFormatUtils.parseUTCTimestamp("2022-04-23T10:33:00.123456Z");
        for (int k = 0; k < 61; k++) {
            final TimestampSampler sampler = createTimestampSampler(k, 'm', sink);
            final long bucketSize = Timestamps.MINUTE_MICROS * (k == 0 ? 1 : k);
            final long expectedTs = ts - ts % bucketSize;
            for (int i = 0; i < (int) (bucketSize / Timestamps.SECOND_MICROS); i += 4) {
                long actualTs = sampler.round(expectedTs + i * Timestamps.SECOND_MICROS);
                if (expectedTs != actualTs) {
                    Assert.fail(String.format(
                            "Failed at: %s, i: %d. Expected: %s, actual: %s",
                            sink, i, Timestamps.toString(expectedTs), Timestamps.toString(actualTs))
                    );
                }
            }
        }
    }

    @Test
    public void testMissingInterval() {
        assertFailure(92, "missing interval", null, 92);
    }

    @Test
    public void testNegativeInterval() {
        assertFailure(110, "negative interval is not allowed", "-1m", 110);
    }

    @Test
    public void testNoQualifier() {
        assertFailure(100, "expected interval qualifier", "45", 98);
    }

    @Test
    public void testParseInterval() throws SqlException {
        Assert.assertEquals(1, TimestampSamplerFactory.parseInterval("1m", 1, 0));

        try {
            TimestampSamplerFactory.parseInterval("0m", 1, 0);
        } catch (SqlException e) {
            Assert.assertEquals(0, e.getPosition());
            TestUtils.assertContains(e.getFlyweightMessage(), "zero is not a valid sample value");
        }

        try {
            TimestampSamplerFactory.parseInterval("fm", 1, 0);
        } catch (SqlException e) {
            Assert.assertEquals(0, e.getPosition());
            TestUtils.assertContains(e.getFlyweightMessage(), "invalid sample value");
        }
    }

    @Test
    public void testSeconds() throws NumericException, SqlException {
        final StringSink sink = new StringSink();
        final long ts = TimestampFormatUtils.parseUTCTimestamp("2022-04-23T10:33:00.123456Z");
        for (int k = 0; k < 61; k++) {
            final TimestampSampler sampler = createTimestampSampler(k, 's', sink);
            final long bucketSize = Timestamps.SECOND_MICROS * (k == 0 ? 1 : k);
            final long expectedTs = ts - ts % bucketSize;
            for (int i = 0; i < (int) (bucketSize / Timestamps.SECOND_MICROS); i++) {
                long actualTs = sampler.round(expectedTs + i * Timestamps.SECOND_MICROS);
                if (expectedTs != actualTs) {
                    Assert.fail(String.format(
                            "Failed at: %s, i: %d. Expected: %s, actual: %s",
                            sink, i, Timestamps.toString(expectedTs), Timestamps.toString(actualTs))
                    );
                }
            }
        }
    }

    @Test
    public void testSecondsZero() {
        assertFailure(120, "zero is not a valid sample value", "0s", 120);
    }

    @Test
    public void testUnsupportedQualifier() {
        assertFailure(130, "unsupported interval qualifier", "K", 130);
    }

    @Test
    public void testUnsupportedQualifier2() {
        assertFailure(132, "unsupported interval qualifier", "21K", 130);
    }

    private static void assertFindIntervalEndIndexFailure(int expectedPosition, CharSequence expectedMessage, CharSequence sampleBy, int position) {
        try {
            TimestampSamplerFactory.findIntervalEndIndex(sampleBy, position);
            Assert.fail();
        } catch (SqlException e) {
            Assert.assertEquals(expectedPosition, e.getPosition());
            TestUtils.assertContains(e.getFlyweightMessage(), expectedMessage);
        }
    }

    private static TimestampSampler createTimestampSampler(int bucketSize, char units, StringSink sink) throws SqlException {
        sink.clear();
        if (bucketSize > 0) {
            sink.put(bucketSize);
        }
        sink.put(units);
        TimestampSampler sampler = TimestampSamplerFactory.getInstance(sink, 120);
        Assert.assertNotNull(sampler);
        return sampler;
    }

    private void assertFailure(int expectedPosition, CharSequence expectedMessage, CharSequence sampleBy, int position) {
        try {
            TimestampSamplerFactory.getInstance(sampleBy, position);
            Assert.fail();
        } catch (SqlException e) {
            Assert.assertEquals(expectedPosition, e.getPosition());
            TestUtils.assertContains(e.getFlyweightMessage(), expectedMessage);
        }
    }
}