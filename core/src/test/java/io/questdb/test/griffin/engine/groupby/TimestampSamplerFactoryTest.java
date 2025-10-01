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

import io.questdb.cairo.TimestampDriver;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.groupby.TimestampSampler;
import io.questdb.griffin.engine.groupby.TimestampSamplerFactory;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.TestTimestampType;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class TimestampSamplerFactoryTest {
    private final TestTimestampType timestampType;

    public TimestampSamplerFactoryTest(TestTimestampType timestampType) {
        this.timestampType = timestampType;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> testParams() {
        return Arrays.asList(new Object[][]{
                {TestTimestampType.MICRO}, {TestTimestampType.NANO}
        });
    }

    @Test
    public void testFindIntervalEndIndex() throws SqlException {
        assertFindIntervalEndIndexFailure(1, "missing interval", null, 1);
        assertFindIntervalEndIndexFailure(1_002, "expected interval qualifier", "45", 1000);
        assertFindIntervalEndIndexFailure(42, "expected interval qualifier", "", 42);
        assertFindIntervalEndIndexFailure(50, "expected single letter qualifier", "1bar", 49);
        assertFindIntervalEndIndexFailure(100, "negative interval is not allowed", "-", 100);

        Assert.assertEquals(0, TimestampSamplerFactory.findIntervalEndIndex("m", 11, "sample"));
    }

    @Test
    public void testLongQualifier() {
        try {
            TimestampSamplerFactory.getInstance(timestampType.getDriver(), "1sa", 130);
            Assert.fail();
        } catch (SqlException e) {
            Assert.assertEquals(131, e.getPosition());
            TestUtils.assertContains(e.getFlyweightMessage(), "expected single letter qualifier");
        }
    }

    @Test
    public void testMicros() throws NumericException, SqlException {
        final StringSink sink = new StringSink();
        final TimestampDriver driver = timestampType.getDriver();
        final long ts = driver.parseFloorLiteral("2022-04-23T10:33:00.123456Z");
        final Rnd rand = new Rnd();
        for (int j = 0; j < 1000; j++) {
            final int k = rand.nextInt(1001);
            final TimestampSampler sampler = createTimestampSampler(k, 'U', sink);
            final long bucketSize = driver.fromMicros(k == 0 ? 1 : k);
            final long expectedTs = ts - ts % bucketSize;
            for (int i = 0; i < bucketSize; i += 4) {
                long actualTs = sampler.round(expectedTs + i);
                if (expectedTs != actualTs) {
                    Assert.fail(String.format(
                                    "Failed at: %s, i: %d. Expected: %s, actual: %s",
                                    sink, i, driver.toMSecString(expectedTs), driver.toMSecString(actualTs)
                            )
                    );
                }
            }
        }
    }

    @Test
    public void testMillis() throws NumericException, SqlException {
        final StringSink sink = new StringSink();
        final TimestampDriver driver = timestampType.getDriver();
        final long ts = driver.parseFloorLiteral("2022-04-23T10:33:00.123456Z");
        for (int k = 0; k < 101; k++) {
            final TimestampSampler sampler = createTimestampSampler(k, 'T', sink);
            final long bucketSize = driver.fromMillis(k == 0 ? 1 : k);
            final long expectedTs = ts - ts % bucketSize;
            for (int i = 0; i < bucketSize; i += 40) {
                long actualTs = sampler.round(expectedTs + i);
                if (expectedTs != actualTs) {
                    Assert.fail(String.format(
                                    "Failed at: %s, i: %d. Expected: %s, actual: %s",
                                    sink, i, driver.toMSecString(expectedTs), driver.toMSecString(actualTs)
                            )
                    );
                }
            }
        }
    }

    @Test
    public void testMinutes() throws NumericException, SqlException {
        final StringSink sink = new StringSink();
        final TimestampDriver driver = timestampType.getDriver();
        final long ts = driver.parseFloorLiteral("2022-04-23T10:33:00.123456Z");
        for (int k = 0; k < 61; k++) {
            final TimestampSampler sampler = createTimestampSampler(k, 'm', sink);
            final long bucketSize = driver.fromMinutes(k == 0 ? 1 : k);
            final long expectedTs = ts - ts % bucketSize;
            for (int i = 0; i < (int) (bucketSize / driver.fromSeconds(1)); i += 4) {
                long actualTs = sampler.round(expectedTs + driver.fromSeconds(i));
                if (expectedTs != actualTs) {
                    Assert.fail(String.format(
                                    "Failed at: %s, i: %d. Expected: %s, actual: %s",
                                    sink, i, driver.toMSecString(expectedTs), driver.toMSecString(actualTs)
                            )
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
        Assert.assertEquals(1, TimestampSamplerFactory.parseInterval("1m", 1, 0, "sample", Numbers.INT_NULL, ' '));

        try {
            TimestampSamplerFactory.parseInterval("0m", 1, 0, "sample", Numbers.INT_NULL, ' ');
        } catch (SqlException e) {
            Assert.assertEquals(0, e.getPosition());
            TestUtils.assertContains(e.getFlyweightMessage(), "zero is not a valid sample value");
        }

        try {
            TimestampSamplerFactory.parseInterval("fm", 1, 0, "sample", Numbers.INT_NULL, ' ');
        } catch (SqlException e) {
            Assert.assertEquals(0, e.getPosition());
            TestUtils.assertContains(e.getFlyweightMessage(), "invalid sample value");
        }
    }

    @Test
    public void testSeconds() throws NumericException, SqlException {
        final StringSink sink = new StringSink();
        final TimestampDriver driver = timestampType.getDriver();
        final long ts = driver.parseFloorLiteral("2022-04-23T10:33:00.123456Z");
        for (int k = 0; k < 61; k++) {
            final TimestampSampler sampler = createTimestampSampler(k, 's', sink);
            final long bucketSize = driver.fromSeconds(k == 0 ? 1 : k);
            final long expectedTs = ts - ts % bucketSize;
            for (int i = 0; i < (int) (bucketSize / driver.fromSeconds(1)); i++) {
                long actualTs = sampler.round(expectedTs + driver.fromSeconds(i));
                if (expectedTs != actualTs) {
                    Assert.fail(String.format(
                            "Failed at: %s, i: %d. Expected: %s, actual: %s",
                            sink, i, driver.toMSecString(expectedTs), driver.toMSecString(actualTs)
                    ));
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
            TimestampSamplerFactory.findIntervalEndIndex(sampleBy, position, "sample");
            Assert.fail();
        } catch (SqlException e) {
            Assert.assertEquals(expectedPosition, e.getPosition());
            TestUtils.assertContains(e.getFlyweightMessage(), expectedMessage);
        }
    }

    private void assertFailure(int expectedPosition, CharSequence expectedMessage, CharSequence sampleBy, int position) {
        try {
            final TimestampDriver driver = timestampType.getDriver();
            TimestampSamplerFactory.getInstance(driver, sampleBy, position);
            Assert.fail();
        } catch (SqlException e) {
            Assert.assertEquals(expectedPosition, e.getPosition());
            TestUtils.assertContains(e.getFlyweightMessage(), expectedMessage);
        }
    }

    private TimestampSampler createTimestampSampler(int bucketSize, char units, StringSink sink) throws SqlException {
        sink.clear();
        if (bucketSize > 0) {
            sink.put(bucketSize);
        }
        sink.put(units);
        final TimestampDriver driver = timestampType.getDriver();
        final TimestampSampler sampler = TimestampSamplerFactory.getInstance(driver, sink, 120);
        Assert.assertNotNull(sampler);
        return sampler;
    }
}
