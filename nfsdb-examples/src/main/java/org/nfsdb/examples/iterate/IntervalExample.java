/*
 * Copyright (c) 2014-2015. Vlad Ilyushchenko
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

import com.nfsdb.journal.Journal;
import com.nfsdb.journal.JournalWriter;
import com.nfsdb.journal.PartitionType;
import com.nfsdb.journal.exceptions.JournalException;
import com.nfsdb.journal.factory.JournalFactory;
import com.nfsdb.journal.factory.configuration.JournalConfigurationBuilder;
import com.nfsdb.journal.utils.Dates;
import com.nfsdb.journal.utils.Dates2;
import com.nfsdb.journal.utils.Files;
import org.nfsdb.examples.model.Quote;
import org.nfsdb.examples.support.QuoteGenerator;

import java.io.File;
import java.util.concurrent.TimeUnit;

public class IntervalExample {
    public static void main(String[] args) throws JournalException {
        if (args.length != 1) {
            System.out.println("Usage: " + IntervalExample.class.getName() + " <path>");
            System.exit(1);
        }
        String journalLocation = args[0];
        try (JournalFactory factory = new JournalFactory(new JournalConfigurationBuilder() {{
            $(Quote.class)
                    .recordCountHint(5000000) // hint that journal is going to be big
                    .partitionBy(PartitionType.MONTH) // partition by MONTH
                    .$ts() // tell factory that Quote has "timestamp" column. If column is called differently you can pass its name
            ;
        }}.build(journalLocation))) {

            // delete existing quote journal
            Files.delete(new File(factory.getConfiguration().getJournalBase(), Quote.class.getName()));

            // get some data in :)
            try (JournalWriter<Quote> w = factory.bulkWriter(Quote.class)) {
                QuoteGenerator.generateQuoteData(w, 10000000, 90);
            }

            // basic iteration
            try (Journal<Quote> journal = factory.reader(Quote.class)) {
                int count = 0;
                long t = System.nanoTime();

                long lo = Dates2.addDays(System.currentTimeMillis(), 10);
                long hi = Dates2.addDays(lo, 10);

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
