package org.nfsdb.examples.iterate;

import com.nfsdb.journal.Journal;
import com.nfsdb.journal.JournalWriter;
import com.nfsdb.journal.exceptions.JournalException;
import com.nfsdb.journal.factory.JournalFactory;
import com.nfsdb.journal.utils.Dates;
import org.nfsdb.examples.model.Quote;
import org.nfsdb.examples.support.QuoteGenerator;

import java.util.concurrent.TimeUnit;

public class IntervalExample {
    public static void main(String[] args) throws JournalException {
        if (args.length != 1) {
            System.out.println("Usage: " + IntervalExample.class.getName() + " <path>");
            System.exit(1);
        }
        String journalLocation = args[0];
        // this is another way to setup JournalFactory if you would like to provide NullsAdaptor. NullsAdaptor for thrift,
        // which is used in this case implements JIT-friendly object reset method, which is quite fast.
        try (JournalFactory factory = new JournalFactory(journalLocation)) {

            // get some data in :)
            try (JournalWriter<Quote> w = factory.writer(Quote.class)) {
                QuoteGenerator.generateQuoteData(w, 10000000, 90);
            }

            // basic iteration
            try (Journal<Quote> journal = factory.reader(Quote.class)) {
                int count = 0;
                long t = System.nanoTime();

                // 10 days from now
                long lo = System.currentTimeMillis() + 10 * 24 * 60 * 60 * 1000;
                // 20 days from now
                long hi = lo + 10 * 24 * 60 * 60 * 1000;

                // iterate the interval between lo and hi millis.
                for (Quote q : journal.query().all().iterator(Dates.interval(lo, hi))) {
                    assert q != null;
                    count++;
                }
                System.out.println("Iterator read " + count + " quotes in " + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t) + "ms.");
            }
        }

    }

}
