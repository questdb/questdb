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
import com.nfsdb.exp.RecordSourcePrinter;
import com.nfsdb.exp.StringSink;
import com.nfsdb.factory.configuration.JournalConfigurationBuilder;
import com.nfsdb.lang.cst.RowSource;
import com.nfsdb.lang.cst.impl.fltr.DoubleGreaterThanRowFilter;
import com.nfsdb.lang.cst.impl.fltr.SymbolEqualsRowFilter;
import com.nfsdb.lang.cst.impl.jsrc.JournalSourceImpl;
import com.nfsdb.lang.cst.impl.ksrc.PartialSymbolKeySource;
import com.nfsdb.lang.cst.impl.ksrc.SymbolKeySource;
import com.nfsdb.lang.cst.impl.psrc.IntervalPartitionSource;
import com.nfsdb.lang.cst.impl.psrc.JournalPartitionSource;
import com.nfsdb.lang.cst.impl.qry.Record;
import com.nfsdb.lang.cst.impl.qry.RecordSource;
import com.nfsdb.lang.cst.impl.ref.StringRef;
import com.nfsdb.lang.cst.impl.rsrc.FilteredRowSource;
import com.nfsdb.lang.cst.impl.rsrc.KvIndexHeadRowSource;
import com.nfsdb.lang.cst.impl.rsrc.KvIndexRowSource;
import com.nfsdb.lang.cst.impl.rsrc.UnionRowSource;
import com.nfsdb.model.Album;
import com.nfsdb.model.Band;
import com.nfsdb.model.Quote;
import com.nfsdb.test.tools.JournalTestFactory;
import com.nfsdb.test.tools.TestData;
import com.nfsdb.utils.Files;
import com.nfsdb.utils.Interval;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;

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
        final String expected = "2013-03-14T22:20:00.000Z\tADM.L\t0.222782760571\t0.713311797139\t273855268\t1241896809\tFast trading\tSK\t\n" +
                "2013-03-14T11:13:20.000Z\tBT-A.L\t0.054623153514\t0.022790647791\t1665489604\t1825275267\tFast trading\tLXE\t\n" +
                "2013-03-14T07:03:20.000Z\tRRS.L\t0.159645354181\t0.976660790706\t1450742524\t160829557\tFast trading\tLXE\t\n" +
                "2013-03-14T19:33:20.000Z\tTLW.L\t0.063977755512\t0.985843973249\t1057856100\t846789630\tFast trading\tSK\t\n" +
                "2013-03-14T18:10:00.000Z\tAGK.L\t0.116478223467\t0.068878315768\t1229861726\t1864880804\tFast trading\tLXE\t\n" +
                "2013-03-13T10:13:20.000Z\tGKN.L\t0.512305033003\t0.035398506629\t150521667\t340900279\tFast trading\tSK\t\n" +
                "2013-03-14T23:43:20.000Z\tABF.L\t0.353620839777\t0.303265005916\t1628633600\t812948041\tFast trading\tSK\t\n" +
                "2013-03-14T20:56:40.000Z\tWTB.L\t0.241157059812\t0.893600171171\t1497753802\t43031921\tFast trading\tLXE\t\n" +
                "2013-03-14T12:36:40.000Z\tBP.L\t0.655503453217\t0.564446918384\t1419046110\t950920455\tFast trading\tSK\t\n" +
                "2013-03-14T04:16:40.000Z\tLLOY.L\t0.660138895170\t0.757562030419\t1968010178\t461392040\tFast trading\tGR\t\n";

        StringRef sym = new StringRef("sym");
        // from quote head by sym where timestamp in ("2013-03-12T00:00:00.000Z", "2013-03-15T00:00:00.000Z")
        assertEquals(expected,
                new JournalSourceImpl(
                        new IntervalPartitionSource(
                                new JournalPartitionSource(journal, false)
                                , new Interval("2013-03-12T00:00:00.000Z", "2013-03-15T00:00:00.000Z")
                        ),
                        new KvIndexHeadRowSource(
                                sym,
                                new SymbolKeySource(sym)
                                , 1
                                , 0
                                , null
                        )
                )
        );
    }

    @Test
    public void testHeadAfterFilter() throws Exception {

        final String expected = "2013-03-14T22:20:00.000Z\tADM.L\t0.222782760571\t0.713311797139\t273855268\t1241896809\tFast trading\tSK\t\n" +
                "2013-03-13T13:00:00.000Z\tRRS.L\t0.723572282002\t0.389559162769\t414242867\t1526964673\tFast trading\tSK\t\n" +
                "2013-03-14T19:33:20.000Z\tTLW.L\t0.063977755512\t0.985843973249\t1057856100\t846789630\tFast trading\tSK\t\n" +
                "2013-03-13T10:13:20.000Z\tGKN.L\t0.512305033003\t0.035398506629\t150521667\t340900279\tFast trading\tSK\t\n" +
                "2013-03-14T23:43:20.000Z\tABF.L\t0.353620839777\t0.303265005916\t1628633600\t812948041\tFast trading\tSK\t\n" +
                "2013-03-14T09:50:00.000Z\tWTB.L\t0.695525766035\t0.835285696284\t1022464172\t1295894299\tFast trading\tSK\t\n" +
                "2013-03-14T12:36:40.000Z\tBP.L\t0.655503453217\t0.564446918384\t1419046110\t950920455\tFast trading\tSK\t\n" +
                "2013-03-13T14:23:20.000Z\tLLOY.L\t0.491420610092\t0.691007955236\t1742184590\t1402169094\tFast trading\tSK\t\n";

        StringRef sym = new StringRef("sym");
        StringRef ex = new StringRef("ex");
        StringRef exValue = new StringRef("SK");

        assertEquals(expected,
                new JournalSourceImpl(
                        new IntervalPartitionSource(
                                new JournalPartitionSource(
                                        journal
                                        , false
                                )
                                , new Interval("2013-03-12T00:00:00.000Z", "2013-03-15T00:00:00.000Z"))
                        ,
                        new KvIndexHeadRowSource(sym, new SymbolKeySource(sym), 1, 0, new SymbolEqualsRowFilter(ex, exValue))
                )
        );

    }

    @Test
    public void testUnionAndFilter() throws Exception {

        final String expected = "2013-03-12T13:23:20.000Z\tBP.L\t0.754494592594\t0.069385533063\t1360360213\t403082481\tFast trading\tSK\t\n" +
                "2013-03-12T07:50:00.000Z\tWTB.L\t0.588779883603\t0.245365403760\t1675014181\t2093945023\tFast trading\tGR\t\n";
        StringRef sym = new StringRef("sym");

        // from quote where timestamp in ("2013-03-12T00:00:00.000Z", "2013-03-13T00:00:00.000Z") and (sym in ("BP.L", "XXX") or sym = "WTB.L")
        assertEquals(expected, new JournalSourceImpl(new IntervalPartitionSource(new JournalPartitionSource(journal, false), new Interval("2013-03-12T00:00:00.000Z", "2013-03-13T00:00:00.000Z")), new FilteredRowSource(
                        new UnionRowSource(
                                new RowSource[]{
                                        new KvIndexRowSource(
                                                sym
                                                ,
                                                new PartialSymbolKeySource(sym, new ArrayList<String>() {{
                                                    add("BP.L");
                                                    add("XXX");
                                                }})
                                        ), new KvIndexRowSource(
                                        sym
                                        ,
                                        new PartialSymbolKeySource(sym, new ArrayList<String>() {{
                                            add("WTB.L");
                                        }})
                                )}
                        )
                        , new DoubleGreaterThanRowFilter("bid", 0.4)
                ))
        );
    }

    private void assertEquals(CharSequence expected, RecordSource<? extends Record> src) {
        new RecordSourcePrinter(sink).print(src);
        Assert.assertEquals(expected, sink.toString());
        sink.flush();
    }
}
