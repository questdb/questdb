package org.nfsdb.examples.iterate;

import com.nfsdb.journal.Journal;
import com.nfsdb.journal.JournalWriter;
import com.nfsdb.journal.exceptions.JournalException;
import com.nfsdb.journal.factory.JournalConfiguration;
import com.nfsdb.journal.factory.JournalFactory;
import com.nfsdb.journal.iterators.ConcurrentIterator;
import com.nfsdb.thrift.ThriftNullsAdaptorFactory;
import org.nfsdb.examples.model.Quote;
import org.nfsdb.examples.support.QuoteGenerator;

import java.io.File;
import java.util.concurrent.TimeUnit;

public class IteratorExample {

    public static void main(String[] args) throws JournalException {
        if (args.length != 1) {
            System.out.println("Usage: " + IteratorExample.class.getName() + " <path>");
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

            // basic iteration
            try (Journal<Quote> journal = factory.reader(Quote.class)) {
                int count = 0;
                long t = System.nanoTime();

                // regular iterator
                for (Quote q : journal) {
                    assert q != null;
                    count++;
                }
                System.out.println("Iterator read " + count + " quotes in " + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t) + "ms.");

                // buffered iterator, this one reuses object it produces making no impact on GC
                // it you intend to throw away majority of objects in the loop it is best to use buffered iterator
                count = 0;
                t = System.nanoTime();
                for (Quote q : journal.bufferedIterator()) {
                    assert q != null;
                    count++;
                }
                System.out.println("Buffered iterator read " + count + " quotes in " + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t) + "ms.");

                // concurrent iterator just as buffered iterator reuses object instance. Main difference is that all reading is done in another thread
                // e.g in parallel with execution on main loop. Parallel iterator is sensitive to buffer size.
                // because parallel iterator starts a thread it has to be closed after use
                //
                // there is an overhead to messaging between threads, but it would pay dividends if the for loop is either CPU or IO bound as both
                // read and compute operations will be done in parallel.
                count = 0;
                t = System.nanoTime();
                try (ConcurrentIterator<Quote> iterator = journal.concurrentIterator().buffer(1024 * 64)) {
                    for (Quote q : iterator) {
                        assert q != null;
                        count++;
                    }
                }
                System.out.println("Parallel iterator read " + count + " quotes in " + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t) + "ms.");
            }
        }

    }
}
