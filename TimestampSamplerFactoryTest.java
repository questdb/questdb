/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.griffin.engine.groupby;

import io.questdb.griffin.SqlException;
import io.questdb.std.NumericException;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class TimestampSamplerFactoryTest {

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
    public void testMinutes() throws NumericException, SqlException {
        StringSink sink = new StringSink();
        for (int k = 0; k < 61; k++) {
            sink.clear();
            if (k > 0) {
                sink.put(k).put('m');
            } else {
                sink.put('m');
            }
            TimestampSampler sampler = TimestampSamplerFactory.getInstance(sink, 120);
            Assert.assertNotNull(sampler);

            final long n = Timestamps.MINUTE_MICROS * (k == 0 ? 1 : k);
            long timestamp = TimestampFormatUtils.parseUTCTimestamp("2018-04-15T10:23:00.000000Z");
            timestamp = timestamp - timestamp % n;
            for (int i = 0; i < 60; i++) {
                long actual = sampler.round(timestamp + i * Timestamps.SECOND_MICROS);
                if (timestamp != actual) {
                    Assert.fail("Failed at: " + sink + ". Expected: " + Timestamps.toString(timestamp) + ", actual: " + Timestamps.toString(actual));
                }
            }
        }
    }

    @Test
    public void testMissingInterval() {
        assertFailure(92, "missing interval", null, 92);
    }

    @Test
    public void testNoQualifier() {
        assertFailure(100, "expected interval qualifier", "45", 98);
    }

    @Test
    public void testSeconds() throws NumericException, SqlException {
        StringSink sink = new StringSink();
        for (int k = 0; k < 61; k++) {
            sink.clear();
            if (k > 0) {
                sink.put(k).put('s');
            } else {
                sink.put('s');
            }
            TimestampSampler sampler = TimestampSamplerFactory.getInstance(sink, 120);
            Assert.assertNotNull(sampler);

            final long n = Timestamps.SECOND_MICROS * (k == 0 ? 1 : k);
            long timestamp = TimestampFormatUtils.parseUTCTimestamp("2018-04-15T10:23:00.000000Z");
            timestamp = timestamp - timestamp % n;
            for (int i = 0; i < n; i += 4) {
                long actual = sampler.round(timestamp + i);
                if (timestamp != actual) {
                    Assert.fail("Failed at: " + sink + ". Expected: " + Timestamps.toString(timestamp) + ", actual: " + Timestamps.toString(actual));
                }
            }
        }
    }

    @Test
    public void testMicros() throws NumericException, SqlException {
        StringSink sink = new StringSink();
        for (int k = 0; k < 61; k++) {
            sink.clear();
            if (k > 0) {
                sink.put(k).put('U');
            } else {
                sink.put('U');
            }
            TimestampSampler sampler = TimestampSamplerFactory.getInstance(sink, 120);
            Assert.assertNotNull(sampler);

            final long n = (k == 0 ? 1 : k);
            long timestamp = TimestampFormatUtils.parseUTCTimestamp("2018-04-15T10:23:00.000000Z");
            timestamp = timestamp - timestamp % n;
            for (int i = 0; i < n; i += 1) {
                long actual = sampler.round(timestamp + i);
                if (timestamp != actual) {
                    Assert.fail("Failed at: " + sink + ". Expected: " + Timestamps.toString(timestamp) + ", actual: " + Timestamps.toString(actual));
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