/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package com.nfsdb.ql.impl.aggregation;

import com.nfsdb.io.sink.StringSink;
import com.nfsdb.misc.Dates;
import com.nfsdb.test.tools.TestUtils;
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
        Dates.appendDateTime(sink, sampler.resample(Dates.parseDateTime("2015-04-03T09:04:45.000Z")));
        TestUtils.assertEquals("2015-04-03T00:00:00.000Z", sink);
    }

    @Test
    public void testHours() throws Exception {
        TimestampSampler sampler = SamplerFactory.from("3h");
        Assert.assertNotNull(sampler);
        Dates.appendDateTime(sink, sampler.resample(Dates.parseDateTime("2015-04-10T10:04:45.000Z")));
        TestUtils.assertEquals("2015-04-10T09:00:00.000Z", sink);
    }

    @Test
    public void testInvalidQualifier() throws Exception {
        Assert.assertNull(SamplerFactory.from("2z"));
    }

    @Test
    public void testMinutes() throws Exception {
        TimestampSampler sampler = SamplerFactory.from("3m");
        Assert.assertNotNull(sampler);
        Dates.appendDateTime(sink, sampler.resample(Dates.parseDateTime("2015-04-10T10:04:45.000Z")));
        TestUtils.assertEquals("2015-04-10T10:03:00.000Z", sink);

        sink.clear();

        Dates.appendDateTime(sink, sampler.resample(Dates.parseDateTime("2015-04-10T10:07:15.000Z")));
        TestUtils.assertEquals("2015-04-10T10:06:00.000Z", sink);
    }

    @Test
    public void testMonths() throws Exception {
        TimestampSampler sampler = SamplerFactory.from("3M");
        Assert.assertNotNull(sampler);
        Dates.appendDateTime(sink, sampler.resample(Dates.parseDateTime("2015-07-03T09:04:45.000Z")));
        TestUtils.assertEquals("2015-07-01T00:00:00.000Z", sink);
    }

    @Test
    public void testNoQualifier() throws Exception {
        Assert.assertNull(SamplerFactory.from("2"));
    }

    @Test
    public void testSeconds() throws Exception {
        TimestampSampler sampler = SamplerFactory.from("15s");
        Assert.assertNotNull(sampler);
        Dates.appendDateTime(sink, sampler.resample(Dates.parseDateTime("2015-04-10T10:04:48.100Z")));
        TestUtils.assertEquals("2015-04-10T10:04:45.000Z", sink);
        sink.clear();
    }

    @Test
    public void testYear() throws Exception {
        TimestampSampler sampler = SamplerFactory.from("Y");
        Assert.assertNotNull(sampler);
        Dates.appendDateTime(sink, sampler.resample(Dates.parseDateTime("2015-07-03T09:04:45.000Z")));
        TestUtils.assertEquals("2015-01-01T00:00:00.000Z", sink);
    }

    @Test
    public void testYears() throws Exception {
        Assert.assertNull(SamplerFactory.from("2Y"));
    }
}
