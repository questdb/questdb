/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2016 Appsicle
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
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 *
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
