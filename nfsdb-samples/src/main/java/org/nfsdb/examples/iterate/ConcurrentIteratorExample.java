package org.nfsdb.examples.iterate;

import com.nfsdb.journal.Journal;
import com.nfsdb.journal.JournalWriter;
import com.nfsdb.journal.exceptions.JournalException;
import com.nfsdb.journal.factory.JournalFactory;
import com.nfsdb.journal.iterators.ConcurrentIterator;
import com.nfsdb.journal.utils.Files;
import com.nfsdb.thrift.JournalThriftFactory;
import org.nfsdb.examples.model.Quote;
import org.nfsdb.examples.support.QuoteGenerator;

import java.io.File;
import java.util.concurrent.TimeUnit;

public class ConcurrentIteratorExample {
    public static void main(String[] args) throws JournalException {

        if (args.length != 1) {
            System.out.println("Usage: " + ConcurrentIteratorExample.class.getName() + " <path>");
            System.exit(1);
        }
        String journalLocation = args[0];

        // this is another way to setup JournalFactory if you would like to provide NullsAdaptor. NullsAdaptor for thrift,
        // which is used in this case implements JIT-friendly object reset method, which is quite fast.
        try (JournalFactory factory = new JournalThriftFactory(journalLocation)) {

            // delete existing quote journal
            Files.delete(new File(factory.getConfiguration().getJournalBase(), "quote"));

            // get some data in :)
            try (JournalWriter<Quote> w = factory.writer(Quote.class)) {
                QuoteGenerator.generateQuoteData(w, 10000000);
            }

            // copying journal using fast BufferedIterator
            try (Journal<Quote> src = factory.reader(Quote.class)) {
                try (JournalWriter<Quote> w = factory.writer(Quote.class, "quote-copy2")) {
                    long t = System.nanoTime();
                    int count = 0;
                    try (ConcurrentIterator<Quote> iterator = src.concurrentIterator()) {
                        for (Quote q : iterator) {
                            w.append(q);
                            count++;
                        }
                    }
                    w.commit();
                    System.out.println("ConcurrentIterator copied " + count + " quotes in " + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t) + "ms.");
                }
            }

            // copying journal using fast BufferedIterator
            try (Journal<Quote> src = factory.reader(Quote.class)) {
                try (JournalWriter<Quote> w = factory.writer(Quote.class, "quote-copy")) {
                    long t = System.nanoTime();
                    int count = 0;
                    for (Quote q : src.bufferedIterator()) {
                        w.append(q);
                        count++;
                    }
                    w.commit();
                    System.out.println("BufferedIterator copied " + count + " quotes in " + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t) + "ms.");
                }
            }

        }
    }
}
