/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2017 Appsicle
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

package com.questdb.ql;

import com.questdb.ex.ParserException;
import org.junit.Test;

import java.io.IOException;

public class ParameterTest extends AbstractAllTypeTest {

    @Test
    public void testEqualDouble() throws Exception {
        try (RecordSource rs = compileSource("abc where eq(d, :value, 0.0000001)")) {
            rs.getParam(":value").set(566.734375);
            assertThat("i\td\tf\tb\tl\tstr\tboo\tsym\tsho\tdate\ttimestamp\n" +
                            "-2041844972\t566.734375000000\t0.7780\t-120\t-6943924477733600060\tXX\tfalse\tBZ\t-24357\t-284210729-04-16T06:37:25.107Z\t2016-05-01T10:22:00.000Z\n",
                    rs, true);
        }

        try (RecordSource rs = compileSource("abc where eq(d, :value, :scale)")) {
            rs.getParam(":value").set(566.734375);
            rs.getParam(":scale").set(0.000001);
            assertThat("i\td\tf\tb\tl\tstr\tboo\tsym\tsho\tdate\ttimestamp\n" +
                            "-2041844972\t566.734375000000\t0.7780\t-120\t-6943924477733600060\tXX\tfalse\tBZ\t-24357\t-284210729-04-16T06:37:25.107Z\t2016-05-01T10:22:00.000Z\n",
                    rs, true);
        }

        try (RecordSource rs = compileSource("abc where eq(d, 566.734375, :scale)")) {
            rs.getParam(":scale").set(0.000001);
            assertThat("i\td\tf\tb\tl\tstr\tboo\tsym\tsho\tdate\ttimestamp\n" +
                            "-2041844972\t566.734375000000\t0.7780\t-120\t-6943924477733600060\tXX\tfalse\tBZ\t-24357\t-284210729-04-16T06:37:25.107Z\t2016-05-01T10:22:00.000Z\n",
                    rs, true);
        }
    }

    @Test
    public void testEqualInt() throws Exception {
        testInt("abc where i = :value");
        testInt("abc where :value = i");
    }

    @Test
    public void testEqualLong() throws Exception {
        try (RecordSource rs = compileSource("abc where l = :value")) {
            rs.getParam(":value").set(-6943924477733600060L);
            assertThat("i\td\tf\tb\tl\tstr\tboo\tsym\tsho\tdate\ttimestamp\n" +
                            "-2041844972\t566.734375000000\t0.7780\t-120\t-6943924477733600060\tXX\tfalse\tBZ\t-24357\t-284210729-04-16T06:37:25.107Z\t2016-05-01T10:22:00.000Z\n",
                    rs, true);
        }
    }

    private void testInt(String query) throws ParserException, IOException {
        try (RecordSource rs = compileSource(query)) {
            rs.getParam(":value").set(1335037859);

            assertThat("i\td\tf\tb\tl\tstr\tboo\tsym\tsho\tdate\ttimestamp\n" +
                            "1335037859\t512.000000000000\t0.8217\t-83\t6574958665733670985\tAX\ttrue\tAX\t5869\t252677978-05-07T03:29:33.753Z\t2016-05-01T10:36:00.000Z\n",
                    rs, true);

            rs.getParam(":value").set(-572338288);
            assertThat("i\td\tf\tb\tl\tstr\tboo\tsym\tsho\tdate\ttimestamp\n" +
                            "-572338288\t632.921875000000\t0.5619\t-114\t7122109662042058469\tKK\tfalse\tAX\t25102\t132059357-01-03T08:27:45.836Z\t2016-05-01T10:32:00.000Z\n",
                    rs, true);
        }
    }
}
