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

package org.nfsdb.examples.query;

import com.nfsdb.journal.Journal;
import com.nfsdb.journal.JournalWriter;
import com.nfsdb.journal.exceptions.JournalException;
import com.nfsdb.journal.factory.JournalFactory;
import com.nfsdb.journal.query.api.QueryHeadBuilder;
import com.nfsdb.journal.utils.Files;
import org.nfsdb.examples.model.Quote;
import org.nfsdb.examples.support.QuoteGenerator;

import java.io.File;
import java.util.concurrent.TimeUnit;

public class HeadQueryBuilderExample {

    public static void main(String[] args) throws JournalException {
        if (args.length != 1) {
            System.out.println("Usage: " + HeadQueryBuilderExample.class.getName() + " <path>");
            System.exit(1);
        }
        String journalLocation = args[0];
        try (JournalFactory factory = new JournalFactory(journalLocation)) {

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
                //
                // search head version of each record for all distinct values of "sym" column
                //
                QueryHeadBuilder<Quote> builder = journal.query().head().withKeys();

                // execute query and consume result set
                for (Quote q : builder.asResultSet().bufferedIterator()) {
                    System.out.println(q);
                    count++;
                }
                System.out.println("Read " + count + " records in " + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t) + "ms");
            }
        }
    }
}
