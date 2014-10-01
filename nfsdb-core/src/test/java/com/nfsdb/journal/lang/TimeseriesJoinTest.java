/*
 * Copyright (c) 2014. Vlad Ilyushchenko
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

package com.nfsdb.journal.lang;

import com.nfsdb.journal.JournalWriter;
import com.nfsdb.journal.exceptions.JournalConfigurationException;
import com.nfsdb.journal.exceptions.JournalRuntimeException;
import com.nfsdb.journal.factory.configuration.JournalConfigurationBuilder;
import com.nfsdb.journal.lang.cst.DataItem;
import com.nfsdb.journal.lang.cst.JoinedSource;
import com.nfsdb.journal.lang.cst.Q;
import com.nfsdb.journal.lang.cst.impl.QImpl;
import com.nfsdb.journal.lang.cst.impl.join.TimeSeriesJoin;
import com.nfsdb.journal.test.tools.JournalTestFactory;
import com.nfsdb.journal.test.tools.Rnd;
import com.nfsdb.journal.utils.Files;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

public class TimeseriesJoinTest {

    @ClassRule
    public static final JournalTestFactory factory;

    static {
        try {
            factory = new JournalTestFactory(
                    new JournalConfigurationBuilder() {{
                        $(Ts.class)
                                .$ts()
                        ;
                    }}.build(Files.makeTempDir())
            );
        } catch (JournalConfigurationException e) {
            throw new JournalRuntimeException(e);
        }

    }

    private static Q q;
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
            ts.timestamp = t1;
            w1.append(ts);

            t2 += rnd.nextPositiveInt() % 100;
            ts.timestamp = t2;
            w2.append(ts);
        }

        w1.commit();
        w2.commit();

        q = new QImpl();
    }

    @Test
    public void testJoinNoNulls() throws Exception {
        String expected = "20~89\n" +
                "20~128\n" +
                "53~89\n" +
                "53~128\n" +
                "53~199\n" +
                "54~89\n" +
                "54~128\n" +
                "54~199\n" +
                "96~128\n" +
                "96~199\n" +
                "102~128\n" +
                "102~199\n" +
                "102~247\n" +
                "118~128\n" +
                "118~199\n" +
                "118~247\n" +
                "132~199\n" +
                "132~247\n" +
                "213~247\n" +
                "213~319\n" +
                "213~322\n" +
                "213~334\n" +
                "229~247\n" +
                "229~319\n" +
                "229~322\n" +
                "229~334\n" +
                "234~247\n" +
                "234~319\n" +
                "234~322\n" +
                "234~334\n";


        JoinedSource src = new TimeSeriesJoin(
                q.forEachPartition(
                        q.source(w1, true)
                        , q.all()
                )
                ,
                q.forEachPartition(
                        q.source(w2, true)
                        , q.all()
                )
                , 150
                , 2 // trigger re-sizes to test ring expand formulas
        );

        StringBuilder builder = new StringBuilder();

        for (DataItem d : src) {
            builder.append(d.partition.getLong(d.rowid, 0));
            builder.append("~");
            if (d.slave == null) {
                builder.append("null");
            } else {
                builder.append(d.slave.partition.getLong(d.slave.rowid, 0));
            }
            builder.append("\n");
        }

        Assert.assertEquals(expected, builder.toString());
    }

    @Test
    public void testJoinWithNulls() throws Exception {

        String expected = "20~null\n" +
                "53~null\n" +
                "54~null\n" +
                "96~null\n" +
                "102~null\n" +
                "118~128\n" +
                "132~null\n" +
                "213~null\n" +
                "229~null\n" +
                "234~247\n";

        JoinedSource src = new TimeSeriesJoin(
                q.forEachPartition(
                        q.source(w1, true)
                        , q.all()
                )
                ,
                q.forEachPartition(
                        q.source(w2, true)
                        , q.all()
                )
                , 15
                , 2 // trigger re-sizes to test ring expand formulas
        );

        StringBuilder builder = new StringBuilder();

        for (DataItem d : src) {
            builder.append(d.partition.getLong(d.rowid, 0));
            builder.append("~");
            if (d.slave == null) {
                builder.append("null");
            } else {
                builder.append(d.slave.partition.getLong(d.slave.rowid, 0));
            }
            builder.append("\n");
        }

        Assert.assertEquals(expected, builder.toString());
    }

    @SuppressWarnings("unused")
    public static class Ts {
        private long timestamp;
    }
}
