/*
 * Copyright (c) 2014-2015. Vlad Ilyushchenko
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
 */

package com.nfsdb.lang;

import com.nfsdb.JournalWriter;
import com.nfsdb.exceptions.JournalConfigurationException;
import com.nfsdb.exceptions.JournalRuntimeException;
import com.nfsdb.export.RecordSourcePrinter;
import com.nfsdb.export.StringSink;
import com.nfsdb.factory.configuration.JournalConfigurationBuilder;
import com.nfsdb.lang.cst.impl.join.TimeSeriesJoin;
import com.nfsdb.lang.cst.impl.jsrc.JournalSourceImpl;
import com.nfsdb.lang.cst.impl.psrc.JournalPartitionSource;
import com.nfsdb.lang.cst.impl.rsrc.AllRowSource;
import com.nfsdb.test.tools.JournalTestFactory;
import com.nfsdb.utils.Files;
import com.nfsdb.utils.Rnd;
import org.junit.*;

public class TimeSeriesJoinTest {

    @ClassRule
    public static final JournalTestFactory factory;

    static {
        try {
            factory = new JournalTestFactory(
                    new JournalConfigurationBuilder() {{
                        $(Ts.class);
                    }}.build(Files.makeTempDir())
            );
        } catch (JournalConfigurationException e) {
            throw new JournalRuntimeException(e);
        }

    }

    private static final StringSink sink = new StringSink();
    private static final RecordSourcePrinter printer = new RecordSourcePrinter(sink);
    private static JournalWriter<Ts> w1;
    private static JournalWriter<Ts> w2;

    @BeforeClass
    public static void setUp() throws Exception {
        w1 = factory.writer(Ts.class, "1");
        w2 = factory.writer(Ts.class, "2");

        Ts ts = new Ts();
        Rnd rnd = new Rnd();

        long t1 = 0;
        long t2 = t1;
        for (int i = 0; i < 10; i++) {
            t1 += rnd.nextPositiveInt() % 100;
            ts.ts = t1;
            w1.append(ts);

            t2 += rnd.nextPositiveInt() % 100;
            ts.ts = t2;
            w2.append(ts);
        }

        w1.commit();
        w2.commit();
    }

    @Before
    public void setUp2() throws Exception {
        sink.clear();
    }

    @Test
    public void testJoinNoNulls() throws Exception {
        String expected = "20\t89\t\n" +
                "20\t128\t\n" +
                "53\t89\t\n" +
                "53\t128\t\n" +
                "53\t199\t\n" +
                "54\t89\t\n" +
                "54\t128\t\n" +
                "54\t199\t\n" +
                "96\t128\t\n" +
                "96\t199\t\n" +
                "102\t128\t\n" +
                "102\t199\t\n" +
                "102\t247\t\n" +
                "118\t128\t\n" +
                "118\t199\t\n" +
                "118\t247\t\n" +
                "132\t199\t\n" +
                "132\t247\t\n" +
                "213\t247\t\n" +
                "213\t319\t\n" +
                "213\t322\t\n" +
                "213\t334\t\n" +
                "229\t247\t\n" +
                "229\t319\t\n" +
                "229\t322\t\n" +
                "229\t334\t\n" +
                "234\t247\t\n" +
                "234\t319\t\n" +
                "234\t322\t\n" +
                "234\t334\t\n";


        // from w1 tj w2 depth 150

        printer.print(
                new TimeSeriesJoin(
                        new JournalSourceImpl(new JournalPartitionSource(w1, true), new AllRowSource())
                        , 0
                        ,
                        new JournalSourceImpl(new JournalPartitionSource(w2, true), new AllRowSource())
                        , 0
                        , 150
                        , 2 // trigger re-sizes to test ring expand formulas
                )
        );
//        System.out.println(sink);
        Assert.assertEquals(expected, sink.toString());
    }

    @Test
    public void testJoinWithNulls() throws Exception {

        String expected = "20\t\n" +
                "53\t\n" +
                "54\t\n" +
                "96\t\n" +
                "102\t\n" +
                "118\t128\t\n" +
                "132\t\n" +
                "213\t\n" +
                "229\t\n" +
                "234\t247\t\n";

        printer.print(new TimeSeriesJoin(
                new JournalSourceImpl(new JournalPartitionSource(w1, true), new AllRowSource())
                , 0
                ,
                new JournalSourceImpl(new JournalPartitionSource(w2, true), new AllRowSource())
                , 0
                , 15
                , 2 // trigger re-sizes to test ring expand formulas
        ));
        Assert.assertEquals(expected, sink.toString());
    }

    @SuppressWarnings("unused")
    public static class Ts {
        private long ts;
    }
}
