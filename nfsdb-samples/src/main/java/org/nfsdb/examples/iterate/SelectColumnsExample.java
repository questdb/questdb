package org.nfsdb.examples.iterate;

import com.nfsdb.journal.Journal;
import com.nfsdb.journal.JournalWriter;
import com.nfsdb.journal.exceptions.JournalException;
import com.nfsdb.journal.factory.JournalConfiguration;
import com.nfsdb.journal.factory.JournalFactory;
import com.nfsdb.thrift.ThriftNullsAdaptorFactory;
import org.nfsdb.examples.model.Quote;
import org.nfsdb.examples.support.QuoteGenerator;

import java.io.File;
import java.util.concurrent.TimeUnit;

public class SelectColumnsExample {
    public static void main(String[] args) throws JournalException {

        if (args.length != 1) {
            System.out.println("Usage: " + SelectColumnsExample.class.getName() + " <path>");
            System.exit(1);
        }
        String journalLocation = args[0];

        // this is another way to setup JournalFactory if you would like to provide NullsAdaptor. NullsAdaptor for thrift,
        // which is used in this case implements JIT-friendly object reset method, which is quite fast.
        try (JournalFactory factory = new JournalFactory(new JournalConfiguration(new File(journalLocation)).setNullsAdaptorFactory(new ThriftNullsAdaptorFactory()).build())) {

            // get some data in :)
            try (JournalWriter<Quote> w = factory.writer(Quote.class)) {
                QuoteGenerator.generateQuoteData(w, 10000000);
            }

            // read only sym and askSize columns
            try (Journal<Quote> journal = factory.reader(Quote.class).setReadColumns("sym", "askSize")) {
                int count = 0;
                long t = System.nanoTime();
                for (Quote q : journal.bufferedIterator()) {
                    assert q != null;
                    count++;
                }
                System.out.println("Two columns of " + count + " quotes read in " + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t) + "ms.");
            }
        }
    }
}
