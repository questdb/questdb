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

package com.questdb.ql.aggregation;

import com.questdb.std.str.StringSink;
import com.questdb.std.time.DateFormatUtils;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SamplerTest {

    private final StringSink sink = new StringSink();

    @Before
    public void setUp() {
        sink.clear();
    }

    @Test
    public void testDays() throws Exception {
        TimestampSampler sampler = SamplerFactory.from("2d");
        Assert.assertNotNull(sampler);
        DateFormatUtils.appendDateTime(sink, sampler.resample(DateFormatUtils.parseDateTime("2015-04-03T09:04:45.000Z")));
        TestUtils.assertEquals("2015-04-03T00:00:00.000Z", sink);
    }

    @Test
    public void testHours() throws Exception {
        TimestampSampler sampler = SamplerFactory.from("3h");
        Assert.assertNotNull(sampler);
        DateFormatUtils.appendDateTime(sink, sampler.resample(DateFormatUtils.parseDateTime("2015-04-10T10:04:45.000Z")));
        TestUtils.assertEquals("2015-04-10T09:00:00.000Z", sink);
    }

    @Test
    public void testInvalidQualifier() {
        Assert.assertNull(SamplerFactory.from("2z"));
    }

    @Test
    public void testMinutes() throws Exception {
        TimestampSampler sampler = SamplerFactory.from("3m");
        Assert.assertNotNull(sampler);
        DateFormatUtils.appendDateTime(sink, sampler.resample(DateFormatUtils.parseDateTime("2015-04-10T10:04:45.000Z")));
        TestUtils.assertEquals("2015-04-10T10:03:00.000Z", sink);

        sink.clear();

        DateFormatUtils.appendDateTime(sink, sampler.resample(DateFormatUtils.parseDateTime("2015-04-10T10:07:15.000Z")));
        TestUtils.assertEquals("2015-04-10T10:06:00.000Z", sink);
    }

    @Test
    public void testMonths() throws Exception {
        TimestampSampler sampler = SamplerFactory.from("3M");
        Assert.assertNotNull(sampler);
        DateFormatUtils.appendDateTime(sink, sampler.resample(DateFormatUtils.parseDateTime("2015-07-03T09:04:45.000Z")));
        TestUtils.assertEquals("2015-07-01T00:00:00.000Z", sink);
    }

    @Test
    public void testNoQualifier() {
        Assert.assertNull(SamplerFactory.from("2"));
    }

    @Test
    public void testSeconds() throws Exception {
        TimestampSampler sampler = SamplerFactory.from("15s");
        Assert.assertNotNull(sampler);
        DateFormatUtils.appendDateTime(sink, sampler.resample(DateFormatUtils.parseDateTime("2015-04-10T10:04:48.100Z")));
        TestUtils.assertEquals("2015-04-10T10:04:45.000Z", sink);
        sink.clear();
    }

    @Test
    public void testYear() throws Exception {
        TimestampSampler sampler = SamplerFactory.from("Y");
        Assert.assertNotNull(sampler);
        DateFormatUtils.appendDateTime(sink, sampler.resample(DateFormatUtils.parseDateTime("2015-07-03T09:04:45.000Z")));
        TestUtils.assertEquals("2015-01-01T00:00:00.000Z", sink);
    }

    @Test
    public void testYears() {
        Assert.assertNull(SamplerFactory.from("2Y"));
    }
}
