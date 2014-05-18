package org.nfsdb.examples.query;

import com.nfsdb.journal.Journal;
import com.nfsdb.journal.JournalWriter;
import com.nfsdb.journal.exceptions.JournalException;
import com.nfsdb.journal.factory.JournalFactory;
import com.nfsdb.journal.query.api.QueryAllBuilder;
import com.nfsdb.journal.utils.Dates;
import com.nfsdb.journal.utils.Files;
import com.nfsdb.thrift.JournalThriftFactory;
import org.joda.time.DateTime;
import org.nfsdb.examples.model.Quote;
import org.nfsdb.examples.support.QuoteGenerator;

import java.io.File;
import java.util.concurrent.TimeUnit;

public class QueryBuilderExample {

    public static void main(String[] args) throws JournalException {
        if (args.length != 1) {
            System.out.println("Usage: " + QueryBuilderExample.class.getName() + " <path>");
            System.exit(1);
        }
        String journalLocation = args[0];
        // this is another way to setup JournalFactory if you would like to provide NullsAdaptor. NullsAdaptor for thrift,
        // which is used in this case implements JIT-friendly object reset method, which is quite fast.
        try (JournalFactory factory = new JournalThriftFactory(journalLocation)) {

            // delete existing quote journal
            Files.delete(new File(factory.getConfiguration().getJournalBase(), "quote"));

            int count = 10000000;
            long t = System.nanoTime();

            // get some data in :)
            try (JournalWriter<Quote> w = factory.writer(Quote.class)) {
                QuoteGenerator.generateQuoteData(w, count, 90);
            }

            System.out.println("Created " + count + " records in " + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t) + "ms");

            try (Journal<Quote> journal = factory.reader(Quote.class)) {
                count = 0;
                t = System.nanoTime();
                // create query builder to search for all records with key (sym) = "BP.L"
                QueryAllBuilder<Quote> builder = journal.query().all().withKeys("BP.L");

                // execute query and consume result set
                for (Quote q : builder.asResultSet().bufferedIterator()) {
                    assert q != null;
                    count++;
                }
                System.out.println("Read " + count + " records in " + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t) + "ms");

                //
                // reuse builder to narrow down query interval
                //
                DateTime lo = Dates.utc().plusDays(10);
                DateTime hi = lo.plusDays(10);
                t = System.nanoTime();
                count = 0;
                for (Quote q : builder.slice(Dates.interval(lo, hi)).asResultSet().bufferedIterator()) {
                    assert q != null;
                    count++;
                }
                System.out.println("Read " + count + " records in " + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t) + "ms");
            }
        }
    }
}
