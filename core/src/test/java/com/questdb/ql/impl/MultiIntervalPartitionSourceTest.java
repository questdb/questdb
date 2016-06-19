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

import com.questdb.JournalWriter;
import com.questdb.io.RecordSourcePrinter;
import com.questdb.io.sink.StringSink;
import com.questdb.misc.Dates;
import com.questdb.misc.Interval;
import com.questdb.model.Quote;
import com.questdb.ql.impl.interval.MillisIntervalSource;
import com.questdb.ql.impl.interval.MultiIntervalPartitionSource;
import com.questdb.test.tools.AbstractTest;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class MultiIntervalPartitionSourceTest extends AbstractTest {

    @Test
    public void testIntervalMerge() throws Exception {

        JournalWriter<Quote> w = factory.writer(Quote.class);

        TestUtils.generateQuoteData(w, 600, Dates.parseDateTime("2014-03-10T02:00:00.000Z"), Dates.MINUTE_MILLIS);
        w.commit();

        StringSink sink = new StringSink();
        RecordSourcePrinter p = new RecordSourcePrinter(sink);

        p.printCursor(
                new JournalSource(
                        new MultiIntervalPartitionSource(
                                new JournalPartitionSource(w.getMetadata(), true),
                                new MillisIntervalSource(
                                        new Interval("2014-03-10T07:00:00.000Z", "2014-03-10T07:15:00.000Z"),
                                        30 * Dates.MINUTE_MILLIS,
                                        5
                                )
                        ),
                        new AllRowSource()
                ), factory
        );

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
                "2014-03-10T07:15:00.000Z\tBP.L\t642.189208984375\t148.932441711426\t1552494421\t348870719\tFast trading\tLXE\n" +
                "2014-03-10T07:30:00.000Z\tTLW.L\t565.251953125000\t841.436523437500\t1615047121\t644297395\tFast trading\tLXE\n" +
                "2014-03-10T07:31:00.000Z\tBP.L\t832.000000000000\t455.125000000000\t1362399523\t1878389126\tFast trading\tLXE\n" +
                "2014-03-10T07:32:00.000Z\tADM.L\t0.000006987197\t839.667877197266\t338688733\t2070566810\tFast trading\tLXE\n" +
                "2014-03-10T07:33:00.000Z\tABF.L\t103.663425445557\t0.000000434765\t531273286\t348207741\tFast trading\tLXE\n" +
                "2014-03-10T07:34:00.000Z\tBP.L\t0.053261654451\t736.000000000000\t1355509482\t709140220\tFast trading\tLXE\n" +
                "2014-03-10T07:35:00.000Z\tWTB.L\t0.003430566750\t1.196666389704\t1959833467\t11202688\tFast trading\tLXE\n" +
                "2014-03-10T07:36:00.000Z\tBT-A.L\t429.100708007813\t0.000000459957\t529470471\t242586856\tFast trading\tLXE\n" +
                "2014-03-10T07:37:00.000Z\tABF.L\t0.176363535225\t52.099388122559\t1598572037\t1996360238\tFast trading\tLXE\n" +
                "2014-03-10T07:38:00.000Z\tWTB.L\t0.000062718613\t1008.554687500000\t1048335333\t909143501\tFast trading\tLXE\n" +
                "2014-03-10T07:39:00.000Z\tADM.L\t0.619109332561\t928.000000000000\t1965884596\t1738115144\tFast trading\tLXE\n" +
                "2014-03-10T07:40:00.000Z\tTLW.L\t852.791519165039\t0.001275104150\t520184564\t1486216751\tFast trading\tLXE\n" +
                "2014-03-10T07:41:00.000Z\tLLOY.L\t15.792054176331\t536.433593750000\t609456541\t1550087499\tFast trading\tLXE\n" +
                "2014-03-10T07:42:00.000Z\tWTB.L\t768.000000000000\t219.500000000000\t1587309098\t1365025789\tFast trading\tLXE\n" +
                "2014-03-10T07:43:00.000Z\tBP.L\t685.250000000000\t670.320312500000\t26628947\t1772718488\tFast trading\tLXE\n" +
                "2014-03-10T07:44:00.000Z\tADM.L\t260.000000000000\t0.111657716334\t531328298\t906370597\tFast trading\tLXE\n" +
                "2014-03-10T07:45:00.000Z\tLLOY.L\t297.791748046875\t59.238281250000\t933551814\t47787516\tFast trading\tLXE\n" +
                "2014-03-10T08:00:00.000Z\tADM.L\t173.568359375000\t37.507137298584\t1871533495\t1603161207\tFast trading\tLXE\n" +
                "2014-03-10T08:01:00.000Z\tBT-A.L\t88.671875000000\t0.000000382630\t1189497143\t760190233\tFast trading\tLXE\n" +
                "2014-03-10T08:02:00.000Z\tBP.L\t540.550628662109\t0.000000008101\t1806647964\t1894050117\tFast trading\tLXE\n" +
                "2014-03-10T08:03:00.000Z\tABF.L\t260.469482421875\t61.015514373779\t1321850879\t1204224486\tFast trading\tLXE\n" +
                "2014-03-10T08:04:00.000Z\tABF.L\t470.720214843750\t0.002475794172\t1997992393\t1316891827\tFast trading\tLXE\n" +
                "2014-03-10T08:05:00.000Z\tBP.L\t0.229545563459\t12.000000000000\t1794768723\t185908900\tFast trading\tLXE\n" +
                "2014-03-10T08:06:00.000Z\tBT-A.L\t939.570312500000\t0.007751652505\t1797224541\t2103392386\tFast trading\tLXE\n" +
                "2014-03-10T08:07:00.000Z\tBP.L\t0.038459967822\t992.000000000000\t140057010\t1108533019\tFast trading\tLXE\n" +
                "2014-03-10T08:08:00.000Z\tGKN.L\t0.000000008103\t0.002171463799\t1478626497\t1753864214\tFast trading\tLXE\n" +
                "2014-03-10T08:09:00.000Z\tADM.L\t26.801757812500\t936.000000000000\t1525601344\t640122392\tFast trading\tLXE\n" +
                "2014-03-10T08:10:00.000Z\tADM.L\t0.928103029728\t0.000000004512\t838729240\t684086147\tFast trading\tLXE\n" +
                "2014-03-10T08:11:00.000Z\tRRS.L\t256.000000000000\t0.000000047101\t1834834044\t624982627\tFast trading\tLXE\n" +
                "2014-03-10T08:12:00.000Z\tRRS.L\t512.000000000000\t717.875000000000\t316094146\t1369157616\tFast trading\tLXE\n" +
                "2014-03-10T08:13:00.000Z\tADM.L\t236.897296905518\t413.407714843750\t1381486578\t864537248\tFast trading\tLXE\n" +
                "2014-03-10T08:14:00.000Z\tTLW.L\t384.000000000000\t0.006601167843\t1557414502\t1895628378\tFast trading\tLXE\n" +
                "2014-03-10T08:15:00.000Z\tRRS.L\t1024.000000000000\t0.000022280823\t1278648509\t1022403670\tFast trading\tLXE\n" +
                "2014-03-10T08:30:00.000Z\tABF.L\t0.000000034360\t0.000000000000\t91266345\t866204593\tFast trading\tLXE\n" +
                "2014-03-10T08:31:00.000Z\tBT-A.L\t171.355381011963\t0.000000009602\t652660990\t1444192781\tFast trading\tLXE\n" +
                "2014-03-10T08:32:00.000Z\tRRS.L\t0.491918861866\t72.625000000000\t769068522\t1919100715\tFast trading\tLXE\n" +
                "2014-03-10T08:33:00.000Z\tADM.L\t114.882701873779\t0.000002514900\t1812360042\t500977037\tFast trading\tLXE\n" +
                "2014-03-10T08:34:00.000Z\tWTB.L\t512.832031250000\t512.000000000000\t891627643\t1040917954\tFast trading\tLXE\n" +
                "2014-03-10T08:35:00.000Z\tBP.L\t213.250000000000\t0.000913336349\t866746313\t249805048\tFast trading\tLXE\n" +
                "2014-03-10T08:36:00.000Z\tWTB.L\t896.000000000000\t0.000025470589\t251197254\t1742792730\tFast trading\tLXE\n" +
                "2014-03-10T08:37:00.000Z\tTLW.L\t470.000000000000\t11.135848045349\t834333169\t171793346\tFast trading\tLXE\n" +
                "2014-03-10T08:38:00.000Z\tLLOY.L\t256.000000000000\t0.002955231466\t1884029053\t1184939285\tFast trading\tLXE\n" +
                "2014-03-10T08:39:00.000Z\tWTB.L\t1013.625000000000\t422.500000000000\t1637478680\t916917546\tFast trading\tLXE\n" +
                "2014-03-10T08:40:00.000Z\tWTB.L\t0.001305471727\t0.055660916492\t1790034058\t1987835306\tFast trading\tLXE\n" +
                "2014-03-10T08:41:00.000Z\tBT-A.L\t10.425106048584\t609.250000000000\t433094689\t1094588493\tFast trading\tLXE\n" +
                "2014-03-10T08:42:00.000Z\tADM.L\t152.000000000000\t508.197265625000\t186002027\t2071374618\tFast trading\tLXE\n" +
                "2014-03-10T08:43:00.000Z\tBT-A.L\t0.036184009165\t1.155909627676\t361116821\t316613958\tFast trading\tLXE\n" +
                "2014-03-10T08:44:00.000Z\tABF.L\t0.000001974584\t0.000150146036\t1385379069\t1977169214\tFast trading\tLXE\n" +
                "2014-03-10T08:45:00.000Z\tBP.L\t864.000000000000\t0.630369469523\t1194557424\t2013834487\tFast trading\tLXE\n" +
                "2014-03-10T09:00:00.000Z\tBP.L\t0.001495893754\t0.000005770782\t99560093\t1439860473\tFast trading\tLXE\n" +
                "2014-03-10T09:01:00.000Z\tAGK.L\t279.437500000000\t822.000000000000\t596185212\t1124656535\tFast trading\tLXE\n" +
                "2014-03-10T09:02:00.000Z\tABF.L\t968.953125000000\t1019.551757812500\t568267787\t2059950664\tFast trading\tLXE\n" +
                "2014-03-10T09:03:00.000Z\tAGK.L\t656.384887695313\t0.123030025512\t1638798442\t1263864613\tFast trading\tLXE\n" +
                "2014-03-10T09:04:00.000Z\tGKN.L\t0.001112255559\t0.000069882813\t748300897\t2110403420\tFast trading\tLXE\n" +
                "2014-03-10T09:05:00.000Z\tGKN.L\t6.000000000000\t0.000029743338\t498371749\t590920697\tFast trading\tLXE\n" +
                "2014-03-10T09:06:00.000Z\tBT-A.L\t0.000127211417\t1024.000000000000\t1674123731\t693424325\tFast trading\tLXE\n" +
                "2014-03-10T09:07:00.000Z\tBT-A.L\t0.000000007170\t968.000000000000\t1927738320\t413500238\tFast trading\tLXE\n" +
                "2014-03-10T09:08:00.000Z\tTLW.L\t639.983398437500\t916.312500000000\t1750861894\t1570425854\tFast trading\tLXE\n" +
                "2014-03-10T09:09:00.000Z\tGKN.L\t0.000084723393\t0.000019912064\t469731283\t721417386\tFast trading\tLXE\n" +
                "2014-03-10T09:10:00.000Z\tTLW.L\t0.000022475878\t0.000001168705\t1706037937\t2069367849\tFast trading\tLXE\n" +
                "2014-03-10T09:11:00.000Z\tWTB.L\t0.000000001841\t0.000000012859\t465098055\t477196959\tFast trading\tLXE\n" +
                "2014-03-10T09:12:00.000Z\tLLOY.L\t0.286881171167\t7.250000000000\t209405535\t522631168\tFast trading\tLXE\n" +
                "2014-03-10T09:13:00.000Z\tABF.L\t0.134603418410\t0.053931683302\t1163515138\t255656666\tFast trading\tLXE\n" +
                "2014-03-10T09:14:00.000Z\tTLW.L\t448.000000000000\t0.000000051594\t1226339480\t2082585491\tFast trading\tLXE\n" +
                "2014-03-10T09:15:00.000Z\tADM.L\t0.000004317501\t28.000000000000\t1030704795\t884042452\tFast trading\tLXE\n";

        Assert.assertEquals(expected, sink.toString());

    }
}