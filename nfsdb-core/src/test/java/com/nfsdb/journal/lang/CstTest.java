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

import com.nfsdb.journal.Journal;
import com.nfsdb.journal.JournalWriter;
import com.nfsdb.journal.Partition;
import com.nfsdb.journal.column.FixedColumn;
import com.nfsdb.journal.column.SymbolTable;
import com.nfsdb.journal.factory.configuration.JournalConfigurationBuilder;
import com.nfsdb.journal.lang.cst.*;
import com.nfsdb.journal.lang.cst.impl.QImpl;
import com.nfsdb.journal.lang.cst.impl.ref.IntRef;
import com.nfsdb.journal.lang.cst.impl.ref.StringRef;
import com.nfsdb.journal.model.Quote;
import com.nfsdb.journal.test.tools.JournalTestFactory;
import com.nfsdb.journal.test.tools.TestData;
import com.nfsdb.journal.test.tools.TestUtils;
import com.nfsdb.journal.utils.Dates;
import com.nfsdb.journal.utils.Files;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;

public class CstTest {

    @ClassRule
    public static final JournalTestFactory factory = new JournalTestFactory(
            new JournalConfigurationBuilder() {{
                $(Quote.class)
                        .$sym("sym").index().valueCountHint(15)
                        .$sym("ex").index().valueCountHint(10)
                        .$str("mode")
                        .$ts()
                ;

            }}.build(Files.makeTempDir())
    );
    private static final Q q = new QImpl();
    private static JournalWriter<Quote> w;

    @BeforeClass
    public static void setUp() throws Exception {
        w = factory.writer(Quote.class);
        TestData.appendQuoteData2(w);
    }

    @Test
    public void testUnionAndFilter() throws Exception {
        StringRef sym = new StringRef();
        sym.value = "sym";


        DataSource<Quote> ds =
                q.ds(
                        q.forEachPartition(
                                q.interval(
                                        q.source(w, false)
                                        , Dates.interval("2013-03-12T00:00:00.000Z", "2013-03-13T00:00:00.000Z")
                                )
                                , q.forEachRow(
                                        q.union(
                                                q.kvSource(sym
                                                        , q.symbolTableSource(sym, new ArrayList<String>() {{
                                                                    add("BP.L");
                                                                    add("XXX");
                                                                }}
                                                        )
                                                )
                                                , q.kvSource(sym
                                                        , q.symbolTableSource(sym, new ArrayList<String>() {{
                                                                    add("WTB.L");
                                                                }}
                                                        )
                                                )
                                        )
                                        , q.greaterThan("ask", 0.6)
                                )
                        )
                        , new Quote()
                );

        for (Quote quote : ds) {
            System.out.println(quote);
        }
    }

    @Test
    public void testHead() throws Exception {
        StringRef sym = new StringRef("sym");

        DataSource<Quote> ds =
                q.ds(
                        q.forEachPartition(
                                q.interval(
                                        q.source(w, false)
                                        , Dates.interval("2013-03-12T00:00:00.000Z", "2013-03-15T00:00:00.000Z")
                                )
                                , q.kvSource(sym, q.symbolTableSource(sym), 1, 0, null)
                        ), new Quote()
                );

        for (Quote quote : ds) {
            System.out.println(quote);
        }
    }

    @Test
    public void testHeadFiltered() throws Exception {
        StringRef sym = new StringRef("sym");
        StringRef ex = new StringRef("ex");
        StringRef exValue = new StringRef("SK");

        DataSource<Quote> ds =
                q.ds(
                        q.forEachPartition(
                                q.interval(
                                        q.source(w, false)
                                        , Dates.interval("2013-03-12T00:00:00.000Z", "2013-03-15T00:00:00.000Z")
                                )
                                , q.kvSource(sym, q.symbolTableSource(sym), 1, 0, q.equalsSymbol(ex, exValue))
                        ), new Quote()
                );

        for (Quote quote : ds) {
            System.out.println(quote);
        }
    }

    @Test
    public void testLookup() throws Exception {
        int c = 10000;
        String joinColumn = "sym";
        JournalWriter<Quote> w = factory.writer(Quote.class, "q1", c);
        TestUtils.generateQuoteData(w, c);
        w.close();

        Journal<Quote> master = factory.reader(Quote.class, "q1", c);
        Journal<Quote> slave = factory.reader(Quote.class, "q1", c);
        SymbolTable masterTab = master.getSymbolTable(joinColumn);


        //////////////////

        IntRef key = new IntRef();
        StringRef sym = new StringRef(joinColumn);
        JournalSource src = q.forEachPartition(
                q.source(slave, false)
                , q.kvSource(sym
                        , q.singleKeySource(key)
                        , 1000, 0, null
                )
        );
        SymbolTable slaveTab = src.getJournal().getSymbolTable(joinColumn);
        ////////////////////

        int map[] = new int[masterTab.size()];


        Partition last = null;
        FixedColumn col = null;
        int colIndex = -1;

        long t = System.nanoTime();
        for (int i = 0; i < 1; i++) {

            Arrays.fill(map, -1);

            for (DataItem d : q.forEachPartition(q.source(master, false), q.all())) {

                if (last != d.partition) {
                    last = d.partition;
                    if (colIndex == -1) {
                        colIndex = d.partition.getJournal().getMetadata().getColumnIndex(joinColumn);
                    }
                    col = (FixedColumn) d.partition.getAbstractColumn(colIndex);
                }

                assert col != null;

                int masterKey = col.getInt(d.rowid);
                int slaveKey = map[masterKey];

                if (slaveKey == -1) {
                    slaveKey = slaveTab.getQuick(masterTab.value(masterKey));
                    map[masterKey] = slaveKey;
                }

                key.value = slaveKey;
                src.reset();
                int count = 0;
                for (DataItem di : src) {
                    count++;
                }
//                System.out.println(count);
            }
        }
        System.out.println(System.nanoTime() - t);
    }

    @Test
    public void testJoin() throws Exception {

        int c = 10000;
        JournalWriter<Quote> w = factory.writer(Quote.class, "q", c);
        TestUtils.generateQuoteData(w, c);
        w.close();

        Journal<Quote> master = factory.reader(Quote.class, "q", c);
        Journal<Quote> slave = factory.reader(Quote.class, "q", c);

        StringRef sym = new StringRef("sym");
        IntRef key = new IntRef();
        JoinedSource src = q.join(
                q.forEachPartition(
                        q.source(master, false)
                        , q.all()
                )
                , sym
                , q.forEachPartition(
                        q.source(slave, false)
                        , q.kvSource(sym
                                , q.singleKeySource(key)
                                , 1000, 0, null
                        )
                )
                , sym
                , key
                , null
        );

        long count = 0;
        long t = 0;
        for (int i = -2; i < 10; i++) {
            if (i == 0) {
                t = System.nanoTime();
                count = 0;
            }
            src.reset();
            for (JoinedData d : src) {
                count++;
            }
        }
        System.out.println(count);
        System.out.println(System.nanoTime() - t);

    }
}
