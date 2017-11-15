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

import com.questdb.model.Quote;
import com.questdb.ql.interval.MultiIntervalPartitionSource;
import com.questdb.std.LongList;
import com.questdb.std.str.StringSink;
import com.questdb.std.time.DateFormatUtils;
import com.questdb.std.time.Dates;
import com.questdb.store.JournalWriter;
import com.questdb.test.tools.AbstractTest;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class MultiIntervalPartitionSourceTest extends AbstractTest {

    @Test
    public void testIntervalMerge() throws Exception {
        StringSink sink = new StringSink();

        try (JournalWriter<Quote> w = getFactory().writer(Quote.class)) {
            TestUtils.generateQuoteData(w, 600, DateFormatUtils.parseDateTime("2014-03-10T02:00:00.000Z"), Dates.MINUTE_MILLIS);
            w.commit();

            RecordSourcePrinter p = new RecordSourcePrinter(sink);

            LongList intervals = new LongList();
            intervals.add(DateFormatUtils.parseDateTime("2014-03-10T07:00:00.000Z"));
            intervals.add(DateFormatUtils.parseDateTime("2014-03-10T07:15:00.000Z"));

            p.print(
                    new JournalRecordSource(
                            new MultiIntervalPartitionSource(
                                    new JournalPartitionSource(w.getMetadata(), true),
                                    intervals
                            ),
                            new AllRowSource()
                    ), getFactory()
            );
        }

        final String expected = "2014-03-10T07:00:00.000Z\tGKN.L\t290.000000000000\t320.000000000000\t1070060020\t627764827\tFast trading\tLXE\n" +
                "2014-03-10T07:01:00.000Z\tLLOY.L\t0.001271238521\t0.000000010817\t855783502\t444545168\tFast trading\tLXE\n" +
                "2014-03-10T07:02:00.000Z\tRRS.L\t0.000010917804\t272.000000000000\t1212565949\t1829154977\tFast trading\tLXE\n" +
                "2014-03-10T07:03:00.000Z\tTLW.L\t245.300086975098\t363.160156250000\t1722093204\t448833342\tFast trading\tLXE\n" +
                "2014-03-10T07:04:00.000Z\tTLW.L\t0.025095539168\t0.000000122058\t1703832336\t180642477\tFast trading\tLXE\n" +
                "2014-03-10T07:05:00.000Z\tADM.L\t902.500000000000\t24.333267211914\t781438951\t502201118\tFast trading\tLXE\n" +
                "2014-03-10T07:06:00.000Z\tAGK.L\t144.695419311523\t0.000039814179\t639071723\t1848238665\tFast trading\tLXE\n" +
                "2014-03-10T07:07:00.000Z\tLLOY.L\t0.000035416079\t15.248794555664\t1987214795\t856360285\tFast trading\tLXE\n" +
                "2014-03-10T07:08:00.000Z\tAGK.L\t0.207015849650\t0.199165701866\t1090730005\t1076974002\tFast trading\tLXE\n" +
                "2014-03-10T07:09:00.000Z\tLLOY.L\t447.510742187500\t209.001678466797\t136979290\t653726755\tFast trading\tLXE\n" +
                "2014-03-10T07:10:00.000Z\tBT-A.L\t662.032958984375\t0.000000007138\t1140333902\t1156896957\tFast trading\tLXE\n" +
                "2014-03-10T07:11:00.000Z\tAGK.L\t512.000000000000\t33.973937988281\t1723438228\t349327821\tFast trading\tLXE\n" +
                "2014-03-10T07:12:00.000Z\tWTB.L\t384.000000000000\t0.000000832384\t2145991300\t1388483923\tFast trading\tLXE\n" +
                "2014-03-10T07:13:00.000Z\tTLW.L\t0.000000093063\t0.000071085584\t1186156647\t1143726003\tFast trading\tLXE\n" +
                "2014-03-10T07:14:00.000Z\tAGK.L\t0.006215140224\t0.000000004051\t2086874501\t1272052914\tFast trading\tLXE\n" +
                "2014-03-10T07:15:00.000Z\tBP.L\t642.189208984375\t148.932441711426\t1552494421\t348870719\tFast trading\tLXE\n";

        Assert.assertEquals(expected, sink.toString());

    }
}