/*******************************************************************************
 *   _  _ ___ ___     _ _
 *  | \| | __/ __| __| | |__
 *  | .` | _|\__ \/ _` | '_ \
 *  |_|\_|_| |___/\__,_|_.__/
 *
 *  Copyright (c) 2014-2015. The NFSdb project and its contributors.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 ******************************************************************************/

package org.nfsdb.examples.query;

import com.nfsdb.Journal;
import com.nfsdb.JournalWriter;
import com.nfsdb.exceptions.JournalException;
import com.nfsdb.factory.JournalFactory;
import com.nfsdb.query.iterator.ConcurrentIterator;
import com.nfsdb.utils.Files;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.nfsdb.examples.model.Quote;
import org.nfsdb.examples.support.QuoteGenerator;

import java.io.File;
import java.util.concurrent.TimeUnit;

public class IteratorExample {

    @SuppressFBWarnings({"CC_CYCLOMATIC_COMPLEXITY"})
    public static void main(String[] args) throws JournalException {
        if (args.length != 1) {
            System.out.println("Usage: " + IteratorExample.class.getName() + " <path>");
            System.exit(1);
        }
        String journalLocation = args[0];

        try (JournalFactory factory = new JournalFactory(journalLocation)) {

            // delete existing quote journal
            Files.delete(new File(factory.getConfiguration().getJournalBase(), Quote.class.getName()));

            // get some data in :)
            try (JournalWriter<Quote> w = factory.bulkWriter(Quote.class)) {
                QuoteGenerator.generateQuoteData(w, 1000000);
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
