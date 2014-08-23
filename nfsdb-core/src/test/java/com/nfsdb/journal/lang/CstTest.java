package com.nfsdb.journal.lang;

import com.nfsdb.journal.JournalWriter;
import com.nfsdb.journal.factory.configuration.JournalConfigurationBuilder;
import com.nfsdb.journal.lang.cst.DataItem;
import com.nfsdb.journal.lang.cst.JournalSource;
import com.nfsdb.journal.lang.cst.Q;
import com.nfsdb.journal.lang.cst.impl.QImpl;
import com.nfsdb.journal.model.Quote;
import com.nfsdb.journal.test.tools.JournalTestFactory;
import com.nfsdb.journal.test.tools.TestUtils;
import com.nfsdb.journal.utils.Dates;
import com.nfsdb.journal.utils.Files;
import org.junit.Rule;
import org.junit.Test;

public class CstTest {

    @Rule
    public final JournalTestFactory factory = new JournalTestFactory(

            new JournalConfigurationBuilder() {{
                $(Quote.class)
                        .$sym("sym").index().valueCountHint(15)
                        .$sym("ex").index().valueCountHint(2)
                        .$str("mode").size(60).index().buckets(5);

            }}.build(Files.makeTempDir())
    );

    @Test
    public void testFilters() throws Exception {


        JournalWriter w = factory.writer(Quote.class);
        TestUtils.generateQuoteData(w, 10000, Dates.interval("2014-01-10T00:00:00.000Z", "2014-01-30T00:00:00.000Z"));


        Q q = new QImpl();

        // for every partition in partition stream execute RowSource
        JournalSource src = q.forEachPartition(

                // stream of partitions that match interval
                // ok
                q.interval(
                        // stream of partitions
                        // ok
                        q.source(w, false)
                        // filtering interval
                        , Dates.interval("2014-01-12T00:00:00.000Z", "2014-01-13T00:00:00.000Z")
                )

                // RowSource that wraps RowCursor from first RowSource param
                // and returns rows that accepted by second RowFilter param
                // ok
                , q.forEachRow(
                        // union of rows of all RowSources
                        // ok
                        q.union(
                                // ok
                                q.kvSource("sym", q.symbolTableSource("sym", "BP.L", "XXX"))
                                //, q.kvSource("sym", q.symbolTableSource("sym", "WTB.L", "XXX"))
                                // or
                                // ok
//                                , q.forEachRow(
//                                        // ok
//                                        q.kvSource("mode", q.hashSource("mode", "Fast trading"))
//                                        // ok
//                                        , q.equalsConst("mode", "Fast trading")
//                                )
                        )
                        // accepts row if all filters accept that row
                        // ok
                        ,
                        q.greaterThan("ask", 0.5)
                )
        );

        long t = System.nanoTime();
        for (DataItem item : src) {
//            System.out.println(item.getPartition().read(item.localRowID()));
            //System.out.println(item);
        }
        System.out.println(System.nanoTime()-t);
    }
}
