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

package org.nfsdb.examples.iterate;

import com.nfsdb.Journal;
import com.nfsdb.JournalWriter;
import com.nfsdb.exceptions.JournalException;
import com.nfsdb.factory.JournalFactory;
import com.nfsdb.query.iterator.ConcurrentIterator;
import com.nfsdb.utils.Files;
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

        try (JournalFactory factory = new JournalFactory(journalLocation)) {

            // delete existing quote journal
            Files.delete(new File(factory.getConfiguration().getJournalBase(), "quote"));
            Files.delete(new File(factory.getConfiguration().getJournalBase(), "quote-copy2"));
            Files.delete(new File(factory.getConfiguration().getJournalBase(), "quote-copy"));

            // get some data in :)
            try (JournalWriter<Quote> w = factory.bulkWriter(Quote.class, "quote")) {
                QuoteGenerator.generateQuoteData(w, 10000000);
            }

            // copying journal using concurrent iterator
            try (Journal<Quote> src = factory.bulkReader(Quote.class, "quote")) {
                try (JournalWriter<Quote> w = factory.bulkWriter(Quote.class, "quote-copy2")) {
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
            try (Journal<Quote> src = factory.bulkReader(Quote.class, "quote")) {
                try (JournalWriter<Quote> w = factory.bulkWriter(Quote.class, "quote-copy")) {
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
