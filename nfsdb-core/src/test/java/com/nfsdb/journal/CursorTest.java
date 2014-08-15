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

package com.nfsdb.journal;

import com.nfsdb.journal.collections.LongArrayList;
import com.nfsdb.journal.exceptions.JournalException;
import com.nfsdb.journal.factory.configuration.JournalConfigurationBuilder;
import com.nfsdb.journal.index.Cursor;
import com.nfsdb.journal.index.StringIndexCursor;
import com.nfsdb.journal.index.experimental.CursorFilter;
import com.nfsdb.journal.index.experimental.FilteredCursor;
import com.nfsdb.journal.index.experimental.filter.IntEqualsFilter;
import com.nfsdb.journal.index.experimental.v2.Q;
import com.nfsdb.journal.model.Quote;
import com.nfsdb.journal.model.Trade;
import com.nfsdb.journal.test.tools.JournalTestFactory;
import com.nfsdb.journal.test.tools.TestUtils;
import com.nfsdb.journal.utils.Dates;
import com.nfsdb.journal.utils.Files;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

public class CursorTest {

    @Rule
    public final JournalTestFactory factory = new JournalTestFactory(

            new JournalConfigurationBuilder() {{
                $(Quote.class)
                        .$sym("sym").index().valueCountHint(15)
                        .$sym("ex").index().valueCountHint(2)
                        .$str("mode").size(60).index();

            }}.build(Files.makeTempDir())
    );

    @Test
    @Ignore
    public void testCST() throws Exception {
        JournalWriter<Quote> w = factory.writer(Quote.class, "quote", 3000000);

        TestUtils.generateQuoteDataRandomMode(w, 3000000);

        Journal<Quote> r = factory.reader(Quote.class);
        long t = 0;
        for (int i = -1000; i < 1000; i++) {
            if (i == 0) {
                t = System.nanoTime();
            }

            Cursor cursor1 = new StringIndexCursor().with("mode", "Fast trading150");
            CursorFilter filter2 = new IntEqualsFilter().with(r.getSymbolTable("sym").getQuick("BP.L")).withColumn("sym");
            final Cursor cursor2 = new FilteredCursor<>(cursor1, filter2);


            LongArrayList result = r.iteratePartitionsDesc(new AbstractResultSetBuilder<Quote, LongArrayList>() {
                @Override
                public void read(long lo, long hi) throws JournalException {
                    cursor2.configure(partition);
                    while (cursor2.hasNext()) {
                        long localRowId = cursor2.next();
                        result.add(localRowId);
                    }
                }

                @Override
                public LongArrayList getResult() {
                    return result;
                }
            });
            System.out.println(result.size());
        }

        System.out.println((System.nanoTime() - t) / 1000);

        Q q = null;

        // select from r where timestamp in (0,0) and (sym in ("BP.L", "XXX") or mode = "Fast trading150") and mode = "test" and ask > 10

        // for every partition in partition stream execute RowSource
        q.forEachPartition(

                // stream of partitions that match interval
                q.interval(
                        // stream of partitions
                        q.source(r)
                        // filtering interval
                        , Dates.interval(0, 0)
                )

                // RowSource that wraps RowCursor from first RowSource param
                // and returns rows that accepted by second RowFilter param
                , q.forEachRow(
                        // union of rows of all RowSources
                        q.union(
                                q.kvSource("sym", q.symbolTableSource("sym", "BP.L", "XXX"))
                                // or
                                , q.forEachRow(
                                        q.kvSource("mode", q.hashSource("Fast trading150"))
                                        , q.equalsConst("mode", "Fast trading150")
                                )
                        )
                        // accepts row if all filters accept that row
                        , q.all(
                                q.equalsConst("mode", "test")
                                , q.greaterThan("ask", 10)
                        )
                )
        );


        // select last 1 by sym from r where timestamp in (0,0) and sym in ("BP.L", "XXX")

        // for every partition in partition stream execute RowSource
        q.forEachPartition(

                // stream of partitions that match interval
                q.interval(
                        // stream of partitions
                        q.source(r)
                        // filtering interval
                        , Dates.interval(0, 0)
                )

                // RowSource that wraps RowCursor from first RowSource param
                // and returns rows that accepted by second RowFilter param
                , q.forEachKv(
                        "sym"
                        , q.lastNGroupByKey(
                                q.symbolTableSource("sym", "BP.L", "XXX")
                                , 1
                        )
                )
        );

        // get latest by "sym" over interval where sym in "BP.L" and "XXX" and ask > 10
        // in this query predicate is executed before taking 1 latest. E.g. if there is there are
        // three records for BP.L and ask:
        //  BP.L 5
        //  BP.L 100
        //  BP.L 50
        // query would get records with ask > 10 and last 1 on that, therefore it'll find BP.L 100


        // for every partition in partition stream execute RowSource
        q.forEachPartition(

                // stream of partitions that match interval
                q.interval(
                        // stream of partitions
                        q.source(r)
                        // filtering interval
                        , Dates.interval(0, 0)
                )

                // RowSource that wraps RowCursor from first RowSource param
                // and returns rows that accepted by second RowFilter param
                , q.forEachKv(
                        "sym"
                        , q.lastNGroupByKey(
                                q.symbolTableSource("sym", "BP.L", "XXX")
                                , 1
                                , q.greaterThan("ask", 10)
                        )
                )
        );


        // almost same as above
        // in this query predicate is executed after taking 1 latest. E.g. if there is there are
        // three records for BP.L and ask:
        //  BP.L 5
        //  BP.L 100
        //  BP.L 50
        // query would get last record (BP.L 5) and apply ask > 10 after that, which results in no records selected.

        // for every partition in partition stream execute RowSource
        q.forEachPartition(

                // stream of partitions that match interval
                q.interval(
                        // stream of partitions
                        q.source(r)
                        // filtering interval
                        , Dates.interval(0, 0)
                )

                // RowSource that wraps RowCursor from first RowSource param
                // and returns rows that accepted by second RowFilter param
                , q.forEachRow(
                        q.forEachKv(
                                "sym"
                                , q.lastNGroupByKey(
                                        q.symbolTableSource("sym", "BP.L", "XXX")
                                        , 1
                                )
                        )
                        , q.greaterThan("ask", 10)
                )
        );

        Journal<Trade> r2 = factory.reader(Trade.class);
        q.join(
                "sym"
                // for every partition in partition stream execute RowSource
                , q.forEachPartition(

                        // stream of partitions that match interval
                        q.interval(
                                // stream of partitions
                                q.source(r)
                                // filtering interval
                                , Dates.interval(0, 0)
                        )

                        // RowSource that wraps RowCursor from first RowSource param
                        // and returns rows that accepted by second RowFilter param
                        , q.forEachRow(
                                q.forEachKv(
                                        "sym"
                                        , q.lastNGroupByKey(
                                                q.symbolTableSource("sym", "BP.L", "XXX")
                                                , 1
                                        )
                                )
                                , q.greaterThan("ask", 10)
                        )
                )
                , q.lastNKeyLookup(
                        "sym"
                        , 1
                        // stream of partitions that match interval
                        , q.interval(
                                // stream of partitions
                                q.source(r2)
                                // filtering interval
                                , Dates.interval(0, 0)
                        )

                )
                , q.equals("bid", "bid")
        );
    }
}
