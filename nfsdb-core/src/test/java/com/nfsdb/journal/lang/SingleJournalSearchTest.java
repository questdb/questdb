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

package com.nfsdb.journal.lang;

import com.nfsdb.journal.JournalWriter;
import com.nfsdb.journal.factory.configuration.JournalConfigurationBuilder;
import com.nfsdb.journal.lang.cst.EntrySource;
import com.nfsdb.journal.lang.cst.RowSource;
import com.nfsdb.journal.lang.cst.impl.fltr.DoubleGreaterThanRowFilter;
import com.nfsdb.journal.lang.cst.impl.fltr.SymbolEqualsRowFilter;
import com.nfsdb.journal.lang.cst.impl.jsrc.JournalSourceImpl;
import com.nfsdb.journal.lang.cst.impl.ksrc.PartialSymbolKeySource;
import com.nfsdb.journal.lang.cst.impl.ksrc.SymbolKeySource;
import com.nfsdb.journal.lang.cst.impl.psrc.IntervalPartitionSource;
import com.nfsdb.journal.lang.cst.impl.psrc.JournalPartitionSource;
import com.nfsdb.journal.lang.cst.impl.ref.StringRef;
import com.nfsdb.journal.lang.cst.impl.rsrc.FilteredRowSource;
import com.nfsdb.journal.lang.cst.impl.rsrc.KvIndexHeadRowSource;
import com.nfsdb.journal.lang.cst.impl.rsrc.KvIndexRowSource;
import com.nfsdb.journal.lang.cst.impl.rsrc.UnionRowSource;
import com.nfsdb.journal.model.Album;
import com.nfsdb.journal.model.Band;
import com.nfsdb.journal.model.Quote;
import com.nfsdb.journal.test.tools.JournalTestFactory;
import com.nfsdb.journal.test.tools.TestData;
import com.nfsdb.journal.utils.Dates;
import com.nfsdb.journal.utils.Files;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
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

    @BeforeClass
    public static void setUp() throws Exception {
        journal = factory.writer(Quote.class, "quote");
        TestData.appendQuoteData2(journal);
    }

    @Test
    public void testUnionAndFilter() throws Exception {

        final String expected = "2013-03-12T13:23:20.000Z\tBP.L\t0.7544945925939783\t0.06938553306251616\t1360360213\t403082481\tFast trading\tSK\t\n" +
                "2013-03-12T07:50:00.000Z\tWTB.L\t0.588779883603474\t0.24536540375950655\t1675014181\t2093945023\tFast trading\tGR\t\n";
        StringRef sym = new StringRef("sym");

        // from quote where timestamp in ("2013-03-12T00:00:00.000Z", "2013-03-13T00:00:00.000Z") and (sym in ("BP.L", "XXX") or sym = "WTB.L")
        assertEquals(expected, new JournalSourceImpl(new IntervalPartitionSource(new JournalPartitionSource(journal, false), Dates.interval("2013-03-12T00:00:00.000Z", "2013-03-13T00:00:00.000Z")), new FilteredRowSource(
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

    @Test
    public void testHead() throws Exception {
        final String expected = "2013-03-14T22:20:00.000Z\tADM.L\t0.22278276057064206\t0.7133117971391447\t273855268\t1241896809\tFast trading\tSK\t\n" +
                "2013-03-14T11:13:20.000Z\tBT-A.L\t0.05462315351407565\t0.022790647790977503\t1665489604\t1825275267\tFast trading\tLXE\t\n" +
                "2013-03-14T07:03:20.000Z\tRRS.L\t0.15964535418102288\t0.9766607907060727\t1450742524\t160829557\tFast trading\tLXE\t\n" +
                "2013-03-14T19:33:20.000Z\tTLW.L\t0.06397775551233298\t0.9858439732492763\t1057856100\t846789630\tFast trading\tSK\t\n" +
                "2013-03-14T18:10:00.000Z\tAGK.L\t0.1164782234673083\t0.06887831576795678\t1229861726\t1864880804\tFast trading\tLXE\t\n" +
                "2013-03-13T10:13:20.000Z\tGKN.L\t0.5123050330030754\t0.03539850662851762\t150521667\t340900279\tFast trading\tSK\t\n" +
                "2013-03-14T23:43:20.000Z\tABF.L\t0.3536208397772588\t0.3032650059161601\t1628633600\t812948041\tFast trading\tSK\t\n" +
                "2013-03-14T20:56:40.000Z\tWTB.L\t0.24115705981206503\t0.8936001711707681\t1497753802\t43031921\tFast trading\tLXE\t\n" +
                "2013-03-14T12:36:40.000Z\tBP.L\t0.655503453216916\t0.564446918383671\t1419046110\t950920455\tFast trading\tSK\t\n" +
                "2013-03-14T04:16:40.000Z\tLLOY.L\t0.6601388951695937\t0.7575620304186355\t1968010178\t461392040\tFast trading\tGR\t\n";

        StringRef sym = new StringRef("sym");
        // from quote head by sym where timestamp in ("2013-03-12T00:00:00.000Z", "2013-03-15T00:00:00.000Z")
        assertEquals(expected,
                new JournalSourceImpl(
                        new IntervalPartitionSource(
                                new JournalPartitionSource(journal, false)
                                , Dates.interval("2013-03-12T00:00:00.000Z", "2013-03-15T00:00:00.000Z")
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

        final String expected = "2013-03-14T22:20:00.000Z\tADM.L\t0.22278276057064206\t0.7133117971391447\t273855268\t1241896809\tFast trading\tSK\t\n" +
                "2013-03-13T13:00:00.000Z\tRRS.L\t0.7235722820016914\t0.3895591627690719\t414242867\t1526964673\tFast trading\tSK\t\n" +
                "2013-03-14T19:33:20.000Z\tTLW.L\t0.06397775551233298\t0.9858439732492763\t1057856100\t846789630\tFast trading\tSK\t\n" +
                "2013-03-13T10:13:20.000Z\tGKN.L\t0.5123050330030754\t0.03539850662851762\t150521667\t340900279\tFast trading\tSK\t\n" +
                "2013-03-14T23:43:20.000Z\tABF.L\t0.3536208397772588\t0.3032650059161601\t1628633600\t812948041\tFast trading\tSK\t\n" +
                "2013-03-14T09:50:00.000Z\tWTB.L\t0.695525766035449\t0.8352856962844901\t1022464172\t1295894299\tFast trading\tSK\t\n" +
                "2013-03-14T12:36:40.000Z\tBP.L\t0.655503453216916\t0.564446918383671\t1419046110\t950920455\tFast trading\tSK\t\n" +
                "2013-03-13T14:23:20.000Z\tLLOY.L\t0.4914206100917844\t0.6910079552361642\t1742184590\t1402169094\tFast trading\tSK\t\n";

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
                                , Dates.interval("2013-03-12T00:00:00.000Z", "2013-03-15T00:00:00.000Z"))
                        ,
                        new KvIndexHeadRowSource(sym, new SymbolKeySource(sym), 1, 0, new SymbolEqualsRowFilter(ex, exValue))
                )
        );

    }


    private void assertEquals(CharSequence expected, EntrySource src) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        JournalEntryPrinter p = new JournalEntryPrinter(bos, true);
        p.print(src);
        Assert.assertEquals(expected, bos.toString());
    }
}
