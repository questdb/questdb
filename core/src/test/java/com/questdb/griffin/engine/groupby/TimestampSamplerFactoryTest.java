/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.griffin.engine.groupby;

import com.questdb.griffin.SqlException;
import com.questdb.std.NumericException;
import com.questdb.std.microtime.DateFormatUtils;
import com.questdb.std.microtime.Dates;
import com.questdb.std.str.StringSink;
import com.questdb.test.tools.TestUtils;
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
            TestUtils.assertContains(e.getMessage(), "expected single letter qualifier");
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

            final long n = Dates.MINUTE_MICROS * (k == 0 ? 1 : k);
            long timestamp = DateFormatUtils.parseTimestamp("2018-04-15T10:23:00.000000Z");
            timestamp = timestamp - timestamp % n;
            for (int i = 0; i < 60; i++) {
                long actual = sampler.round(timestamp + i * Dates.SECOND_MICROS);
                if (timestamp != actual) {
                    Assert.fail("Failed at: " + sink + ". Expected: " + Dates.toString(timestamp) + ", actual: " + Dates.toString(actual));
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

            final long n = Dates.SECOND_MICROS * (k == 0 ? 1 : k);
            long timestamp = DateFormatUtils.parseTimestamp("2018-04-15T10:23:00.000000Z");
            timestamp = timestamp - timestamp % n;
            for (int i = 0; i < n; i += 4) {
                long actual = sampler.round(timestamp + i);
                if (timestamp != actual) {
                    Assert.fail("Failed at: " + sink + ". Expected: " + Dates.toString(timestamp) + ", actual: " + Dates.toString(actual));
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
            TestUtils.assertContains(e.getMessage(), expectedMessage);
        }
    }
}