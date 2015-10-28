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
import com.nfsdb.io.RecordSourcePrinter;
import com.nfsdb.io.sink.StringSink;
import com.nfsdb.model.Quote;
import com.nfsdb.ql.impl.AllRowSource;
import com.nfsdb.ql.impl.JournalPartitionSource;
import com.nfsdb.ql.impl.JournalSource;
import com.nfsdb.ql.impl.ResampledSource;
import com.nfsdb.ql.ops.CountIntAggregatorFunction;
import com.nfsdb.ql.ops.CountLongAggregatorFunction;
import com.nfsdb.ql.ops.FirstDoubleAggregationFunction;
import com.nfsdb.ql.ops.LastDoubleAggregationFunction;
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

    private void assertFunc(final AggregatorFunction func, String expected, boolean print) throws JournalException {
        ResampledSource resampledSource = new ResampledSource(
                new JournalSource(
                        new JournalPartitionSource(r.getMetadata(), false)
                        , new AllRowSource()
                )
                ,
                new ObjList<CharSequence>() {{
                    add("sym");
                }}
                ,
                new ObjList<AggregatorFunction>() {{
                    add(func);
                }}
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
