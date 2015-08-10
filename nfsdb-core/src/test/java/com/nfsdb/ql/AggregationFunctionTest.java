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

package com.nfsdb.ql;

import com.nfsdb.Journal;
import com.nfsdb.JournalWriter;
import com.nfsdb.collections.ObjList;
import com.nfsdb.exceptions.JournalException;
import com.nfsdb.factory.configuration.ColumnMetadata;
import com.nfsdb.io.RecordSourcePrinter;
import com.nfsdb.io.sink.StringSink;
import com.nfsdb.model.Quote;
import com.nfsdb.ql.impl.AllRowSource;
import com.nfsdb.ql.impl.JournalPartitionSource;
import com.nfsdb.ql.impl.JournalSource;
import com.nfsdb.ql.impl.ResampledSource;
import com.nfsdb.ql.ops.*;
import com.nfsdb.test.tools.AbstractTest;
import com.nfsdb.test.tools.TestUtils;
import com.nfsdb.utils.Dates;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class AggregationFunctionTest extends AbstractTest {

    private Journal r;

    @Before
    public void setUp() throws Exception {
        JournalWriter<Quote> w = factory.writer(Quote.class);
        TestUtils.generateQuoteData(w, 1000, Dates.toMillis(2015, 1, 1), 100);
        w.commit();
        r = factory.reader(Quote.class.getName());
    }

    @Test
    public void testAvgDouble() throws Exception {
        assertFunc(
                new AvgDoubleAggregationFunction(r.getMetadata().getColumn("ask"))
                , "52\t7858.423721313780\t151.123533102188\t2015-01-01T00:00:00.000Z\tAGK.L\n" +
                        "66\t14082.423625444866\t213.370054930983\t2015-01-01T00:00:00.000Z\tBP.L\n" +
                        "58\t13335.940655007168\t229.930011293227\t2015-01-01T00:00:00.000Z\tRRS.L\n" +
                        "58\t12905.372450960738\t222.506421568289\t2015-01-01T00:00:00.000Z\tBT-A.L\n" +
                        "65\t13394.330843717370\t206.066628364883\t2015-01-01T00:00:00.000Z\tGKN.L\n" +
                        "54\t13244.428383611292\t245.267192289098\t2015-01-01T00:00:00.000Z\tLLOY.L\n" +
                        "64\t12696.789512222130\t198.387336128471\t2015-01-01T00:00:00.000Z\tABF.L\n" +
                        "66\t13559.914003834442\t205.453242482340\t2015-01-01T00:00:00.000Z\tWTB.L\n" +
                        "51\t15306.804920794536\t300.133429819501\t2015-01-01T00:00:00.000Z\tTLW.L\n" +
                        "66\t16913.363305688258\t256.263080389216\t2015-01-01T00:00:00.000Z\tADM.L\n" +
                        "35\t8486.959649701328\t242.484561420038\t2015-01-01T00:01:00.000Z\tBP.L\n" +
                        "49\t12391.404343216870\t252.885802922793\t2015-01-01T00:01:00.000Z\tRRS.L\n" +
                        "44\t9678.625432634026\t219.968759832592\t2015-01-01T00:01:00.000Z\tWTB.L\n" +
                        "41\t7938.831746992340\t193.630042609569\t2015-01-01T00:01:00.000Z\tGKN.L\n" +
                        "48\t10058.622072452964\t209.554626509437\t2015-01-01T00:01:00.000Z\tAGK.L\n" +
                        "34\t9570.444874542912\t281.483672780674\t2015-01-01T00:01:00.000Z\tBT-A.L\n" +
                        "38\t10483.859246590906\t275.891032805024\t2015-01-01T00:01:00.000Z\tADM.L\n" +
                        "42\t8542.161541170678\t203.384798599302\t2015-01-01T00:01:00.000Z\tTLW.L\n" +
                        "32\t9856.989801879358\t308.030931308730\t2015-01-01T00:01:00.000Z\tABF.L\n" +
                        "37\t9907.802508420520\t267.778446173528\t2015-01-01T00:01:00.000Z\tLLOY.L\n"
                , false
        );

    }

    @Test
    public void testCountInt() throws Exception {
        assertFunc(
                new CountIntAggregatorFunction("count")
                , "52\t2015-01-01T00:00:00.000Z\tAGK.L\n" +
                        "66\t2015-01-01T00:00:00.000Z\tBP.L\n" +
                        "58\t2015-01-01T00:00:00.000Z\tRRS.L\n" +
                        "58\t2015-01-01T00:00:00.000Z\tBT-A.L\n" +
                        "65\t2015-01-01T00:00:00.000Z\tGKN.L\n" +
                        "54\t2015-01-01T00:00:00.000Z\tLLOY.L\n" +
                        "64\t2015-01-01T00:00:00.000Z\tABF.L\n" +
                        "66\t2015-01-01T00:00:00.000Z\tWTB.L\n" +
                        "51\t2015-01-01T00:00:00.000Z\tTLW.L\n" +
                        "66\t2015-01-01T00:00:00.000Z\tADM.L\n" +
                        "35\t2015-01-01T00:01:00.000Z\tBP.L\n" +
                        "49\t2015-01-01T00:01:00.000Z\tRRS.L\n" +
                        "44\t2015-01-01T00:01:00.000Z\tWTB.L\n" +
                        "41\t2015-01-01T00:01:00.000Z\tGKN.L\n" +
                        "48\t2015-01-01T00:01:00.000Z\tAGK.L\n" +
                        "34\t2015-01-01T00:01:00.000Z\tBT-A.L\n" +
                        "38\t2015-01-01T00:01:00.000Z\tADM.L\n" +
                        "42\t2015-01-01T00:01:00.000Z\tTLW.L\n" +
                        "32\t2015-01-01T00:01:00.000Z\tABF.L\n" +
                        "37\t2015-01-01T00:01:00.000Z\tLLOY.L\n"
                , false
        );
    }

    @Test
    public void testCountLong() throws Exception {
        assertFunc(
                new CountLongAggregatorFunction("count")
                , "52\t2015-01-01T00:00:00.000Z\tAGK.L\n" +
                        "66\t2015-01-01T00:00:00.000Z\tBP.L\n" +
                        "58\t2015-01-01T00:00:00.000Z\tRRS.L\n" +
                        "58\t2015-01-01T00:00:00.000Z\tBT-A.L\n" +
                        "65\t2015-01-01T00:00:00.000Z\tGKN.L\n" +
                        "54\t2015-01-01T00:00:00.000Z\tLLOY.L\n" +
                        "64\t2015-01-01T00:00:00.000Z\tABF.L\n" +
                        "66\t2015-01-01T00:00:00.000Z\tWTB.L\n" +
                        "51\t2015-01-01T00:00:00.000Z\tTLW.L\n" +
                        "66\t2015-01-01T00:00:00.000Z\tADM.L\n" +
                        "35\t2015-01-01T00:01:00.000Z\tBP.L\n" +
                        "49\t2015-01-01T00:01:00.000Z\tRRS.L\n" +
                        "44\t2015-01-01T00:01:00.000Z\tWTB.L\n" +
                        "41\t2015-01-01T00:01:00.000Z\tGKN.L\n" +
                        "48\t2015-01-01T00:01:00.000Z\tAGK.L\n" +
                        "34\t2015-01-01T00:01:00.000Z\tBT-A.L\n" +
                        "38\t2015-01-01T00:01:00.000Z\tADM.L\n" +
                        "42\t2015-01-01T00:01:00.000Z\tTLW.L\n" +
                        "32\t2015-01-01T00:01:00.000Z\tABF.L\n" +
                        "37\t2015-01-01T00:01:00.000Z\tLLOY.L\n"
                , false
        );
    }

    @Test
    public void testFirstDouble() throws Exception {
        assertFunc(
                new FirstDoubleAggregationFunction(r.getMetadata().getColumn("bid"))
                , "0.000001189157\t2015-01-01T00:00:00.000Z\tAGK.L\n" +
                        "104.021850585938\t2015-01-01T00:00:00.000Z\tBP.L\n" +
                        "768.000000000000\t2015-01-01T00:00:00.000Z\tRRS.L\n" +
                        "256.000000000000\t2015-01-01T00:00:00.000Z\tBT-A.L\n" +
                        "920.625000000000\t2015-01-01T00:00:00.000Z\tGKN.L\n" +
                        "12.923866510391\t2015-01-01T00:00:00.000Z\tLLOY.L\n" +
                        "0.006530375686\t2015-01-01T00:00:00.000Z\tABF.L\n" +
                        "1.507822513580\t2015-01-01T00:00:00.000Z\tWTB.L\n" +
                        "424.828125000000\t2015-01-01T00:00:00.000Z\tTLW.L\n" +
                        "153.473033905029\t2015-01-01T00:00:00.000Z\tADM.L\n" +
                        "721.375000000000\t2015-01-01T00:01:00.000Z\tBP.L\n" +
                        "17.556239128113\t2015-01-01T00:01:00.000Z\tRRS.L\n" +
                        "0.001319892908\t2015-01-01T00:01:00.000Z\tWTB.L\n" +
                        "375.914062500000\t2015-01-01T00:01:00.000Z\tGKN.L\n" +
                        "0.001517725817\t2015-01-01T00:01:00.000Z\tAGK.L\n" +
                        "570.831542968750\t2015-01-01T00:01:00.000Z\tBT-A.L\n" +
                        "67.250000000000\t2015-01-01T00:01:00.000Z\tADM.L\n" +
                        "0.027357567102\t2015-01-01T00:01:00.000Z\tTLW.L\n" +
                        "0.000000261943\t2015-01-01T00:01:00.000Z\tABF.L\n" +
                        "0.798808738589\t2015-01-01T00:01:00.000Z\tLLOY.L\n"
                , false
        );
    }

    @Test
    public void testFirstLong() throws Exception {
        assertFunc(
                new FirstLongAggregationFunction(r.getMetadata().getTimestampMetadata())
                , "2015-01-01T00:00:00.000Z\t2015-01-01T00:00:00.000Z\tAGK.L\n" +
                        "2015-01-01T00:00:00.100Z\t2015-01-01T00:00:00.000Z\tBP.L\n" +
                        "2015-01-01T00:00:00.300Z\t2015-01-01T00:00:00.000Z\tRRS.L\n" +
                        "2015-01-01T00:00:00.400Z\t2015-01-01T00:00:00.000Z\tBT-A.L\n" +
                        "2015-01-01T00:00:00.500Z\t2015-01-01T00:00:00.000Z\tGKN.L\n" +
                        "2015-01-01T00:00:00.700Z\t2015-01-01T00:00:00.000Z\tLLOY.L\n" +
                        "2015-01-01T00:00:00.800Z\t2015-01-01T00:00:00.000Z\tABF.L\n" +
                        "2015-01-01T00:00:01.200Z\t2015-01-01T00:00:00.000Z\tWTB.L\n" +
                        "2015-01-01T00:00:01.500Z\t2015-01-01T00:00:00.000Z\tTLW.L\n" +
                        "2015-01-01T00:00:01.600Z\t2015-01-01T00:00:00.000Z\tADM.L\n" +
                        "2015-01-01T00:01:00.000Z\t2015-01-01T00:01:00.000Z\tBP.L\n" +
                        "2015-01-01T00:01:00.100Z\t2015-01-01T00:01:00.000Z\tRRS.L\n" +
                        "2015-01-01T00:01:00.200Z\t2015-01-01T00:01:00.000Z\tWTB.L\n" +
                        "2015-01-01T00:01:00.300Z\t2015-01-01T00:01:00.000Z\tGKN.L\n" +
                        "2015-01-01T00:01:00.400Z\t2015-01-01T00:01:00.000Z\tAGK.L\n" +
                        "2015-01-01T00:01:00.500Z\t2015-01-01T00:01:00.000Z\tBT-A.L\n" +
                        "2015-01-01T00:01:00.600Z\t2015-01-01T00:01:00.000Z\tADM.L\n" +
                        "2015-01-01T00:01:00.700Z\t2015-01-01T00:01:00.000Z\tTLW.L\n" +
                        "2015-01-01T00:01:01.200Z\t2015-01-01T00:01:00.000Z\tABF.L\n" +
                        "2015-01-01T00:01:01.700Z\t2015-01-01T00:01:00.000Z\tLLOY.L\n"
                , false
        );
    }

    @Test
    public void testLastDouble() throws Exception {
        assertFunc(
                new LastDoubleAggregationFunction(r.getMetadata().getColumn("bid"))
                , "405.717071533203\t2015-01-01T00:00:00.000Z\tAGK.L\n" +
                        "896.000000000000\t2015-01-01T00:00:00.000Z\tBP.L\n" +
                        "1024.000000000000\t2015-01-01T00:00:00.000Z\tRRS.L\n" +
                        "866.523681640625\t2015-01-01T00:00:00.000Z\tBT-A.L\n" +
                        "0.000024718131\t2015-01-01T00:00:00.000Z\tGKN.L\n" +
                        "7.141985177994\t2015-01-01T00:00:00.000Z\tLLOY.L\n" +
                        "0.000007803264\t2015-01-01T00:00:00.000Z\tABF.L\n" +
                        "0.000000095777\t2015-01-01T00:00:00.000Z\tWTB.L\n" +
                        "778.312500000000\t2015-01-01T00:00:00.000Z\tTLW.L\n" +
                        "0.000004946311\t2015-01-01T00:00:00.000Z\tADM.L\n" +
                        "64.000000000000\t2015-01-01T00:01:00.000Z\tBP.L\n" +
                        "0.000009315784\t2015-01-01T00:01:00.000Z\tRRS.L\n" +
                        "689.399261474609\t2015-01-01T00:01:00.000Z\tWTB.L\n" +
                        "404.315429687500\t2015-01-01T00:01:00.000Z\tGKN.L\n" +
                        "512.000000000000\t2015-01-01T00:01:00.000Z\tAGK.L\n" +
                        "0.000000000000\t2015-01-01T00:01:00.000Z\tBT-A.L\n" +
                        "175.000000000000\t2015-01-01T00:01:00.000Z\tADM.L\n" +
                        "0.000000021643\t2015-01-01T00:01:00.000Z\tTLW.L\n" +
                        "732.000000000000\t2015-01-01T00:01:00.000Z\tABF.L\n" +
                        "4.727073431015\t2015-01-01T00:01:00.000Z\tLLOY.L\n"
                , false
        );
    }

    @Test
    public void testLastLong() throws Exception {
        assertFunc(
                new LastLongAggregationFunction(r.getMetadata().getTimestampMetadata())
                , "2015-01-01T00:00:58.300Z\t2015-01-01T00:00:00.000Z\tAGK.L\n" +
                        "2015-01-01T00:00:59.900Z\t2015-01-01T00:00:00.000Z\tBP.L\n" +
                        "2015-01-01T00:00:59.400Z\t2015-01-01T00:00:00.000Z\tRRS.L\n" +
                        "2015-01-01T00:00:58.500Z\t2015-01-01T00:00:00.000Z\tBT-A.L\n" +
                        "2015-01-01T00:00:56.800Z\t2015-01-01T00:00:00.000Z\tGKN.L\n" +
                        "2015-01-01T00:00:59.800Z\t2015-01-01T00:00:00.000Z\tLLOY.L\n" +
                        "2015-01-01T00:00:59.700Z\t2015-01-01T00:00:00.000Z\tABF.L\n" +
                        "2015-01-01T00:00:58.800Z\t2015-01-01T00:00:00.000Z\tWTB.L\n" +
                        "2015-01-01T00:00:59.600Z\t2015-01-01T00:00:00.000Z\tTLW.L\n" +
                        "2015-01-01T00:00:59.300Z\t2015-01-01T00:00:00.000Z\tADM.L\n" +
                        "2015-01-01T00:01:38.100Z\t2015-01-01T00:01:00.000Z\tBP.L\n" +
                        "2015-01-01T00:01:39.800Z\t2015-01-01T00:01:00.000Z\tRRS.L\n" +
                        "2015-01-01T00:01:39.700Z\t2015-01-01T00:01:00.000Z\tWTB.L\n" +
                        "2015-01-01T00:01:37.800Z\t2015-01-01T00:01:00.000Z\tGKN.L\n" +
                        "2015-01-01T00:01:38.900Z\t2015-01-01T00:01:00.000Z\tAGK.L\n" +
                        "2015-01-01T00:01:39.900Z\t2015-01-01T00:01:00.000Z\tBT-A.L\n" +
                        "2015-01-01T00:01:35.100Z\t2015-01-01T00:01:00.000Z\tADM.L\n" +
                        "2015-01-01T00:01:38.200Z\t2015-01-01T00:01:00.000Z\tTLW.L\n" +
                        "2015-01-01T00:01:37.600Z\t2015-01-01T00:01:00.000Z\tABF.L\n" +
                        "2015-01-01T00:01:38.700Z\t2015-01-01T00:01:00.000Z\tLLOY.L\n"
                , false
        );
    }

    @Test
    public void testSumDouble() throws Exception {
        assertFunc(
                new SumDoubleAggregationFunction(r.getMetadata().getColumn("ask"))
                , "7858.423721313780\t2015-01-01T00:00:00.000Z\tAGK.L\n" +
                        "14082.423625444866\t2015-01-01T00:00:00.000Z\tBP.L\n" +
                        "13335.940655007168\t2015-01-01T00:00:00.000Z\tRRS.L\n" +
                        "12905.372450960738\t2015-01-01T00:00:00.000Z\tBT-A.L\n" +
                        "13394.330843717370\t2015-01-01T00:00:00.000Z\tGKN.L\n" +
                        "13244.428383611292\t2015-01-01T00:00:00.000Z\tLLOY.L\n" +
                        "12696.789512222130\t2015-01-01T00:00:00.000Z\tABF.L\n" +
                        "13559.914003834442\t2015-01-01T00:00:00.000Z\tWTB.L\n" +
                        "15306.804920794536\t2015-01-01T00:00:00.000Z\tTLW.L\n" +
                        "16913.363305688258\t2015-01-01T00:00:00.000Z\tADM.L\n" +
                        "8486.959649701328\t2015-01-01T00:01:00.000Z\tBP.L\n" +
                        "12391.404343216870\t2015-01-01T00:01:00.000Z\tRRS.L\n" +
                        "9678.625432634026\t2015-01-01T00:01:00.000Z\tWTB.L\n" +
                        "7938.831746992340\t2015-01-01T00:01:00.000Z\tGKN.L\n" +
                        "10058.622072452964\t2015-01-01T00:01:00.000Z\tAGK.L\n" +
                        "9570.444874542912\t2015-01-01T00:01:00.000Z\tBT-A.L\n" +
                        "10483.859246590906\t2015-01-01T00:01:00.000Z\tADM.L\n" +
                        "8542.161541170678\t2015-01-01T00:01:00.000Z\tTLW.L\n" +
                        "9856.989801879358\t2015-01-01T00:01:00.000Z\tABF.L\n" +
                        "9907.802508420520\t2015-01-01T00:01:00.000Z\tLLOY.L\n"
                , false
        );
    }

    @Test
    public void testSumInt() throws Exception {
        assertFunc(
                new SumIntAggregationFunction(r.getMetadata().getColumn("askSize"))
                , "936298421\t2015-01-01T00:00:00.000Z\tAGK.L\n" +
                        "1261867203\t2015-01-01T00:00:00.000Z\tBP.L\n" +
                        "-1040054830\t2015-01-01T00:00:00.000Z\tRRS.L\n" +
                        "601364085\t2015-01-01T00:00:00.000Z\tBT-A.L\n" +
                        "1519408602\t2015-01-01T00:00:00.000Z\tGKN.L\n" +
                        "-2134747749\t2015-01-01T00:00:00.000Z\tLLOY.L\n" +
                        "-1868948525\t2015-01-01T00:00:00.000Z\tABF.L\n" +
                        "-605662252\t2015-01-01T00:00:00.000Z\tWTB.L\n" +
                        "-1257342025\t2015-01-01T00:00:00.000Z\tTLW.L\n" +
                        "-955207476\t2015-01-01T00:00:00.000Z\tADM.L\n" +
                        "2085278043\t2015-01-01T00:01:00.000Z\tBP.L\n" +
                        "1108922740\t2015-01-01T00:01:00.000Z\tRRS.L\n" +
                        "-657045748\t2015-01-01T00:01:00.000Z\tWTB.L\n" +
                        "1173213594\t2015-01-01T00:01:00.000Z\tGKN.L\n" +
                        "727741293\t2015-01-01T00:01:00.000Z\tAGK.L\n" +
                        "1816141252\t2015-01-01T00:01:00.000Z\tBT-A.L\n" +
                        "1796333725\t2015-01-01T00:01:00.000Z\tADM.L\n" +
                        "1625426964\t2015-01-01T00:01:00.000Z\tTLW.L\n" +
                        "1887002231\t2015-01-01T00:01:00.000Z\tABF.L\n" +
                        "1314878562\t2015-01-01T00:01:00.000Z\tLLOY.L\n"
                , false
        );
    }


    @Test
    public void testSumIntToLong() throws Exception {
        assertFunc(
                new SumIntToLongAggregationFunction(r.getMetadata().getColumn("askSize"))
                , "56770873269\t2015-01-01T00:00:00.000Z\tAGK.L\n" +
                        "65686376643\t2015-01-01T00:00:00.000Z\tBP.L\n" +
                        "63384454610\t2015-01-01T00:00:00.000Z\tRRS.L\n" +
                        "56435938933\t2015-01-01T00:00:00.000Z\tBT-A.L\n" +
                        "74533852634\t2015-01-01T00:00:00.000Z\tGKN.L\n" +
                        "53699827099\t2015-01-01T00:00:00.000Z\tLLOY.L\n" +
                        "66850528211\t2015-01-01T00:00:00.000Z\tABF.L\n" +
                        "68113814484\t2015-01-01T00:00:00.000Z\tWTB.L\n" +
                        "54577232823\t2015-01-01T00:00:00.000Z\tTLW.L\n" +
                        "72059236556\t2015-01-01T00:00:00.000Z\tADM.L\n" +
                        "32150049115\t2015-01-01T00:01:00.000Z\tBP.L\n" +
                        "56943497588\t2015-01-01T00:01:00.000Z\tRRS.L\n" +
                        "46587594508\t2015-01-01T00:01:00.000Z\tWTB.L\n" +
                        "44122886554\t2015-01-01T00:01:00.000Z\tGKN.L\n" +
                        "47972381549\t2015-01-01T00:01:00.000Z\tAGK.L\n" +
                        "36175879620\t2015-01-01T00:01:00.000Z\tBT-A.L\n" +
                        "40451039389\t2015-01-01T00:01:00.000Z\tADM.L\n" +
                        "44575099924\t2015-01-01T00:01:00.000Z\tTLW.L\n" +
                        "40541707895\t2015-01-01T00:01:00.000Z\tABF.L\n" +
                        "44264551522\t2015-01-01T00:01:00.000Z\tLLOY.L\n"
                , false
        );
    }

    private void assertFunc(final AggregatorFunction func, String expected, boolean print) throws JournalException {
        ResampledSource resampledSource = new ResampledSource(
                new JournalSource(
                        new JournalPartitionSource(r.getMetadata(), false)
                        , new AllRowSource()
                )
                ,
                new ObjList<ColumnMetadata>() {{
                    add(r.getMetadata().getColumn("sym"));
                }}
                ,
                new ObjList<AggregatorFunction>() {{
                    add(func);
                }}
                , r.getMetadata().getTimestampMetadata()
                , ResampledSource.SampleBy.MINUTE
        );

        StringSink sink = new StringSink();
        RecordSourcePrinter out = new RecordSourcePrinter(sink);
        out.printCursor(resampledSource.prepareCursor(factory));
        if (print) {
            System.out.println(sink.toString());
        }
        Assert.assertEquals(expected, sink.toString());
    }
}
