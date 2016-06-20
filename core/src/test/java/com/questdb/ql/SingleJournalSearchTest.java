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

package com.questdb.ql;

import com.questdb.JournalWriter;
import com.questdb.ex.JournalException;
import com.questdb.factory.configuration.JournalConfigurationBuilder;
import com.questdb.io.RecordSourcePrinter;
import com.questdb.io.sink.StringSink;
import com.questdb.misc.Files;
import com.questdb.misc.Interval;
import com.questdb.model.Album;
import com.questdb.model.Band;
import com.questdb.model.Quote;
import com.questdb.ql.impl.JournalPartitionSource;
import com.questdb.ql.impl.JournalSource;
import com.questdb.ql.impl.interval.MultiIntervalPartitionSource;
import com.questdb.ql.impl.interval.SingleIntervalSource;
import com.questdb.ql.impl.latest.KvIndexSymAllHeadRowSource;
import com.questdb.ql.ops.col.SymRecordSourceColumn;
import com.questdb.ql.ops.constant.IntConstant;
import com.questdb.ql.ops.eq.IntEqualsOperator;
import com.questdb.test.tools.JournalTestFactory;
import com.questdb.test.tools.TestData;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;

public class SingleJournalSearchTest {
    @ClassRule
    public static final JournalTestFactory factory = new JournalTestFactory(
            new JournalConfigurationBuilder() {{
                $(Quote.class)
                        .$sym("sym").index().valueCountHint(15)
                        .$sym("ex").index().valueCountHint(10)
                        .$str("mode")
                        .$ts()
                ;

                $(Band.class).recordCountHint(10000)
                        .$sym("name").index().valueCountHint(10000)
                        .$sym("type").index().valueCountHint(5)
                ;

                $(Album.class).recordCountHint(100000)
                        .$sym("band").index().valueCountHint(10000)
                        .$sym("name").index().valueCountHint(100000)
                        .$ts("releaseDate")
                ;
            }}.build(Files.makeTempDir())
    );
    //private static final JournalEntryPrinter out = new JournalEntryPrinter(System.out, true);
    private static JournalWriter<Quote> journal;
    private final StringSink sink = new StringSink();

    @BeforeClass
    public static void setUp() throws Exception {
        journal = factory.writer(Quote.class, "quote");
        TestData.appendQuoteData2(journal);
    }

    @Test
    public void testHead() throws Exception {
        final String expected = "2013-03-13T10:13:20.000Z\tGKN.L\t0.512305033003\t0.035398506629\t150521667\t340900279\tFast trading\tSK\n" +
                "2013-03-14T04:16:40.000Z\tLLOY.L\t0.660138895170\t0.757562030419\t1968010178\t461392040\tFast trading\tGR\n" +
                "2013-03-14T07:03:20.000Z\tRRS.L\t0.159645354181\t0.976660790706\t1450742524\t160829557\tFast trading\tLXE\n" +
                "2013-03-14T11:13:20.000Z\tBT-A.L\t0.054623153514\t0.022790647791\t1665489604\t1825275267\tFast trading\tLXE\n" +
                "2013-03-14T12:36:40.000Z\tBP.L\t0.655503453217\t0.564446918384\t1419046110\t950920455\tFast trading\tSK\n" +
                "2013-03-14T18:10:00.000Z\tAGK.L\t0.116478223467\t0.068878315768\t1229861726\t1864880804\tFast trading\tLXE\n" +
                "2013-03-14T19:33:20.000Z\tTLW.L\t0.063977755512\t0.985843973249\t1057856100\t846789630\tFast trading\tSK\n" +
                "2013-03-14T20:56:40.000Z\tWTB.L\t0.241157059812\t0.893600171171\t1497753802\t43031921\tFast trading\tLXE\n" +
                "2013-03-14T22:20:00.000Z\tADM.L\t0.222782760571\t0.713311797139\t273855268\t1241896809\tFast trading\tSK\n" +
                "2013-03-14T23:43:20.000Z\tABF.L\t0.353620839777\t0.303265005916\t1628633600\t812948041\tFast trading\tSK\n";

        // from quote head by sym where timestamp in ("2013-03-12T00:00:00.000Z", "2013-03-15T00:00:00.000Z")
        assertEquals(expected,
                new JournalSource(
                        new MultiIntervalPartitionSource(
                                new JournalPartitionSource(journal.getMetadata(), true),
                                new SingleIntervalSource(new Interval("2013-03-12T00:00:00.000Z", "2013-03-15T00:00:00.000Z"))
                        ),
                        new KvIndexSymAllHeadRowSource(
                                "sym"
                                , null
                        )
                )
        );
    }

    @Test
    public void testHeadAfterFilter() throws Exception {

        final String expected = "2013-03-13T10:13:20.000Z\tGKN.L\t0.512305033003\t0.035398506629\t150521667\t340900279\tFast trading\tSK\n" +
                "2013-03-13T13:00:00.000Z\tRRS.L\t0.723572282002\t0.389559162769\t414242867\t1526964673\tFast trading\tSK\n" +
                "2013-03-13T14:23:20.000Z\tLLOY.L\t0.491420610092\t0.691007955236\t1742184590\t1402169094\tFast trading\tSK\n" +
                "2013-03-14T09:50:00.000Z\tWTB.L\t0.695525766035\t0.835285696284\t1022464172\t1295894299\tFast trading\tSK\n" +
                "2013-03-14T12:36:40.000Z\tBP.L\t0.655503453217\t0.564446918384\t1419046110\t950920455\tFast trading\tSK\n" +
                "2013-03-14T19:33:20.000Z\tTLW.L\t0.063977755512\t0.985843973249\t1057856100\t846789630\tFast trading\tSK\n" +
                "2013-03-14T22:20:00.000Z\tADM.L\t0.222782760571\t0.713311797139\t273855268\t1241896809\tFast trading\tSK\n" +
                "2013-03-14T23:43:20.000Z\tABF.L\t0.353620839777\t0.303265005916\t1628633600\t812948041\tFast trading\tSK\n";

        IntEqualsOperator filter = (IntEqualsOperator) IntEqualsOperator.FACTORY.newInstance();
        filter.setLhs(new SymRecordSourceColumn(journal.getMetadata().getColumnIndex("ex")));
        filter.setRhs(new IntConstant(journal.getSymbolTable("ex").getQuick("SK")));

        assertEquals(expected,
                new JournalSource(
                        new MultiIntervalPartitionSource(
                                new JournalPartitionSource(
                                        journal.getMetadata()
                                        , true
                                ),
                                new SingleIntervalSource(new Interval("2013-03-12T00:00:00.000Z", "2013-03-15T00:00:00.000Z"))

                        ),
                        new KvIndexSymAllHeadRowSource(
                                "sym",
                                filter
                        )
                )
        );

    }

    private void assertEquals(CharSequence expected, RecordSource src) throws JournalException, IOException {
        new RecordSourcePrinter(sink).print(src, factory);
        Assert.assertEquals(expected, sink.toString());
        sink.flush();
    }
}
