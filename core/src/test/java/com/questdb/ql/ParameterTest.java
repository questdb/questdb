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

    @Test
    public void testSymbolEquals() throws Exception {
        try (RecordSource rs = compileSource("abc where sym = :sym")) {
            rs.getParam(":sym").set("AX");
            assertThat("i\td\tf\tb\tl\tstr\tboo\tsym\tsho\tdate\ttimestamp\n" +
                            "-235358133\t0.000000005555\t0.3509\t-15\t-3107239868490395663\tBZ\tfalse\tAX\t-15331\t18125533-09-05T04:06:38.086Z\t2016-05-01T10:30:00.000Z\n" +
                            "-572338288\t632.921875000000\t0.5619\t-114\t7122109662042058469\tKK\tfalse\tAX\t25102\t132059357-01-03T08:27:45.836Z\t2016-05-01T10:32:00.000Z\n" +
                            "-731466113\t0.000000020896\t0.5433\t-55\t-6626590012581323602\tAX\tfalse\tAX\t-20409\t-253361973-02-05T02:07:37.180Z\t2016-05-01T10:33:00.000Z\n" +
                            "-1966408995\t0.000000014643\t0.6746\t-77\t-8082754367165748693\tXX\ttrue\tAX\t25974\t-225086326-11-23T21:53:40.936Z\t2016-05-01T10:35:00.000Z\n" +
                            "1335037859\t512.000000000000\t0.8217\t-83\t6574958665733670985\tAX\ttrue\tAX\t5869\t252677978-05-07T03:29:33.753Z\t2016-05-01T10:36:00.000Z\n" +
                            "-1383560599\t0.000000157437\t0.6827\t-102\t8889492928577876455\tAX\ttrue\tAX\t-18600\t-196068605-07-20T05:32:43.747Z\t2016-05-01T10:38:00.000Z\n" +
                            "-1475953213\t0.000032060649\t0.4967\t70\t-6071768268784020226\tBZ\tfalse\tAX\t-24455\t-56977607-10-20T13:10:29.515Z\t2016-05-01T10:40:00.000Z\n",
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
