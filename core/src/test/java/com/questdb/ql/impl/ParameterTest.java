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
 ******************************************************************************/

package com.questdb.ql.impl;

import com.questdb.ex.ParserException;
import com.questdb.ql.RecordSource;
import com.questdb.ql.impl.analytic.AbstractAnalyticRecordSourceTest;
import org.junit.Test;

import java.io.IOException;

public class ParameterTest extends AbstractAnalyticRecordSourceTest {

    @Test
    public void testEqualDouble() throws Exception {
        try (RecordSource rs = compileSource("abc where eq(d, :value, 0.0000001)")) {
            rs.getParam(":value").set(566.734375);
            assertThat("i\td\tf\tb\tl\tstr\tboo\tsym\tsho\tdate\ttimestamp\n" +
                            "-2041844972\t566.734375000000\t0.7780\t-120\t-6943924477733600060\tXX\tfalse\tBZ\t-24357\t000-284204894-12-0-2131416T06:37:25.107Z\t2016-05-01T10:22:00.000Z\n",
                    rs, true);
        }

        try (RecordSource rs = compileSource("abc where eq(d, :value, :scale)")) {
            rs.getParam(":value").set(566.734375);
            rs.getParam(":scale").set(0.000001);
            assertThat("i\td\tf\tb\tl\tstr\tboo\tsym\tsho\tdate\ttimestamp\n" +
                            "-2041844972\t566.734375000000\t0.7780\t-120\t-6943924477733600060\tXX\tfalse\tBZ\t-24357\t000-284204894-12-0-2131416T06:37:25.107Z\t2016-05-01T10:22:00.000Z\n",
                    rs, true);
        }

        try (RecordSource rs = compileSource("abc where eq(d, 566.734375, :scale)")) {
            rs.getParam(":scale").set(0.000001);
            assertThat("i\td\tf\tb\tl\tstr\tboo\tsym\tsho\tdate\ttimestamp\n" +
                            "-2041844972\t566.734375000000\t0.7780\t-120\t-6943924477733600060\tXX\tfalse\tBZ\t-24357\t000-284204894-12-0-2131416T06:37:25.107Z\t2016-05-01T10:22:00.000Z\n",
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
                            "-2041844972\t566.734375000000\t0.7780\t-120\t-6943924477733600060\tXX\tfalse\tBZ\t-24357\t000-284204894-12-0-2131416T06:37:25.107Z\t2016-05-01T10:22:00.000Z\n",
                    rs, true);
        }
    }

    @Test
    public void testRegexString() throws Exception {
        try (RecordSource rs = compileSource("abc where str ~ :regex")) {
            rs.getParam(":regex").set("AX");
            assertThat("i\td\tf\tb\tl\tstr\tboo\tsym\tsho\tdate\ttimestamp\n" +
                            "-1153445279\t0.000000567185\t0.0204\t-60\t8416773233910814357\tAX\tfalse\tXX\t-19127\t000-268690388-12-0-2015078T00:06:37.492Z\t2016-05-01T10:24:00.000Z\n" +
                            "1699553881\t-512.000000000000\t0.4848\t-95\t7199909180655756830\tAX\ttrue\tXX\t-15458\t217212983-05-1628908T18:39:59.220Z\t2016-05-01T10:25:00.000Z\n" +
                            "2006313928\t0.675451681018\t0.2969\t-40\t6270672455202306717\tAX\ttrue\tBZ\t-22934\t263834991-01-1978681T18:15:05.778Z\t2016-05-01T10:26:00.000Z\n" +
                            "-1787109293\t0.000076281818\t0.1609\t-69\t-7316123607359392486\tAX\tfalse\tKK\t-19136\t000-230262069-12-0-1726604T22:57:03.970Z\t2016-05-01T10:29:00.000Z\n" +
                            "-731466113\t0.000000020896\t0.5433\t-55\t-6626590012581323602\tAX\tfalse\tAX\t-20409\t000-253356772-01-0-1899589T02:07:37.180Z\t2016-05-01T10:33:00.000Z\n" +
                            "1335037859\t512.000000000000\t0.8217\t-83\t6574958665733670985\tAX\ttrue\tAX\t5869\t252672790-12-1894671T03:29:33.753Z\t2016-05-01T10:36:00.000Z\n" +
                            "-1383560599\t0.000000157437\t0.6827\t-102\t8889492928577876455\tAX\ttrue\tAX\t-18600\t000-196064580-12-0-1470233T05:32:43.747Z\t2016-05-01T10:38:00.000Z\n",
                    rs, true);

            rs.getParam(":regex").set("XX");
            assertThat("i\td\tf\tb\tl\tstr\tboo\tsym\tsho\tdate\ttimestamp\n" +
                            "-2041844972\t566.734375000000\t0.7780\t-120\t-6943924477733600060\tXX\tfalse\tBZ\t-24357\t000-284204894-12-0-2131416T06:37:25.107Z\t2016-05-01T10:22:00.000Z\n" +
                            "-1566901076\t0.000002473130\t0.7274\t-126\t-7387846268299105911\tXX\tfalse\tKK\t-4874\t000-201710735-12-0-1512581T10:14:48.134Z\t2016-05-01T10:31:00.000Z\n" +
                            "-1966408995\t0.000000014643\t0.6746\t-77\t-8082754367165748693\tXX\ttrue\tAX\t25974\t000-225081705-01-0-1687457T21:53:40.936Z\t2016-05-01T10:35:00.000Z\n" +
                            "1598679468\t864.000000000000\t0.3591\t-36\t3446015290144635451\tXX\ttrue\tKK\t-22894\t000-244299855-12-0-1831941T17:50:45.758Z\t2016-05-01T10:37:00.000Z\n",
                    rs, true);
        }
    }

    @Test
    public void testRegexSymbol() throws Exception {
        try (RecordSource rs = compileSource("abc where sym ~ :regex")) {
            rs.getParam(":regex").set("AX");
            assertThat("i\td\tf\tb\tl\tstr\tboo\tsym\tsho\tdate\ttimestamp\n" +
                            "-235358133\t0.000000005555\t0.3509\t-15\t-3107239868490395663\tBZ\tfalse\tAX\t-15331\t18125162-01-135753T04:06:38.086Z\t2016-05-01T10:30:00.000Z\n" +
                            "-572338288\t632.921875000000\t0.5619\t-114\t7122109662042058469\tKK\tfalse\tAX\t25102\t132056646-12-989842T08:27:45.836Z\t2016-05-01T10:32:00.000Z\n" +
                            "-731466113\t0.000000020896\t0.5433\t-55\t-6626590012581323602\tAX\tfalse\tAX\t-20409\t000-253356772-01-0-1899589T02:07:37.180Z\t2016-05-01T10:33:00.000Z\n" +
                            "-1966408995\t0.000000014643\t0.6746\t-77\t-8082754367165748693\tXX\ttrue\tAX\t25974\t000-225081705-01-0-1687457T21:53:40.936Z\t2016-05-01T10:35:00.000Z\n" +
                            "1335037859\t512.000000000000\t0.8217\t-83\t6574958665733670985\tAX\ttrue\tAX\t5869\t252672790-12-1894671T03:29:33.753Z\t2016-05-01T10:36:00.000Z\n" +
                            "-1383560599\t0.000000157437\t0.6827\t-102\t8889492928577876455\tAX\ttrue\tAX\t-18600\t000-196064580-12-0-1470233T05:32:43.747Z\t2016-05-01T10:38:00.000Z\n" +
                            "-1475953213\t0.000032060649\t0.4967\t70\t-6071768268784020226\tBZ\tfalse\tAX\t-24455\t000-56976438-01-0-426674T13:10:29.515Z\t2016-05-01T10:40:00.000Z\n",
                    rs, true);

            rs.getParam(":regex").set("XX");
            assertThat("i\td\tf\tb\tl\tstr\tboo\tsym\tsho\tdate\ttimestamp\n" +
                            "-1532328444\t0.000013792171\t0.5509\t-34\t-6856503215590263904\tKK\tfalse\tXX\t21781\t000-142142802-12-0-1065746T10:07:22.984Z\t2016-05-01T10:23:00.000Z\n" +
                            "-1153445279\t0.000000567185\t0.0204\t-60\t8416773233910814357\tAX\tfalse\tXX\t-19127\t000-268690388-12-0-2015078T00:06:37.492Z\t2016-05-01T10:24:00.000Z\n" +
                            "1699553881\t-512.000000000000\t0.4848\t-95\t7199909180655756830\tAX\ttrue\tXX\t-15458\t217212983-05-1628908T18:39:59.220Z\t2016-05-01T10:25:00.000Z\n" +
                            "-2002373666\t0.332301996648\t0.5725\t53\t-6253307669002054137\tBZ\tfalse\tXX\t-391\t231474144-12-1735467T12:18:04.767Z\t2016-05-01T10:27:00.000Z\n",
                    rs, true);
        }
    }

    private void testInt(String query) throws ParserException, IOException {
        try (RecordSource rs = compileSource(query)) {
            rs.getParam(":value").set(1335037859);

            assertThat("i\td\tf\tb\tl\tstr\tboo\tsym\tsho\tdate\ttimestamp\n" +
                            "1335037859\t512.000000000000\t0.8217\t-83\t6574958665733670985\tAX\ttrue\tAX\t5869\t252672790-12-1894671T03:29:33.753Z\t2016-05-01T10:36:00.000Z\n",
                    rs, true);

            rs.getParam(":value").set(-572338288);
            assertThat("i\td\tf\tb\tl\tstr\tboo\tsym\tsho\tdate\ttimestamp\n" +
                            "-572338288\t632.921875000000\t0.5619\t-114\t7122109662042058469\tKK\tfalse\tAX\t25102\t132056646-12-989842T08:27:45.836Z\t2016-05-01T10:32:00.000Z\n",
                    rs, true);
        }
    }
}
