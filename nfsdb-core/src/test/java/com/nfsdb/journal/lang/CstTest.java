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
import com.nfsdb.journal.factory.configuration.JournalConfigurationBuilder;
import com.nfsdb.journal.lang.cst.DataSource;
import com.nfsdb.journal.lang.cst.Q;
import com.nfsdb.journal.lang.cst.impl.QImpl;
import com.nfsdb.journal.lang.cst.impl.ref.StringRef;
import com.nfsdb.journal.model.Quote;
import com.nfsdb.journal.test.tools.JournalTestFactory;
import com.nfsdb.journal.test.tools.TestData;
import com.nfsdb.journal.utils.Dates;
import com.nfsdb.journal.utils.Files;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;

public class CstTest {

    @ClassRule
    public static final JournalTestFactory factory = new JournalTestFactory(
            new JournalConfigurationBuilder() {{
                $(Quote.class)
                        .$sym("sym").index().valueCountHint(15)
                        .$sym("ex").index().valueCountHint(2)
                        .$str("mode").size(60).index().buckets(5)
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
        StringRef sym = new StringRef();
        sym.value = "sym";

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
}
