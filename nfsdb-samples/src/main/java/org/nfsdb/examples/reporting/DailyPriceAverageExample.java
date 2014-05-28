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

package org.nfsdb.examples.reporting;

import com.nfsdb.journal.Journal;
import com.nfsdb.journal.JournalWriter;
import com.nfsdb.journal.exceptions.JournalException;
import com.nfsdb.journal.factory.JournalConfiguration;
import com.nfsdb.journal.factory.JournalFactory;
import com.nfsdb.journal.printer.JournalPrinter;
import com.nfsdb.journal.printer.appender.StdOutAppender;
import com.nfsdb.journal.query.api.QueryAllBuilder;
import com.nfsdb.journal.utils.Dates;
import com.nfsdb.journal.utils.Files;
import com.nfsdb.thrift.ThriftNullsAdaptorFactory;
import org.joda.time.DateTime;
import org.joda.time.DateTimeField;
import org.joda.time.chrono.ISOChronology;
import org.nfsdb.examples.model.Quote;
import org.nfsdb.examples.support.QuoteGenerator;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class DailyPriceAverageExample {

    public static void main(String[] args) throws JournalException, IOException {
        if (args.length != 1) {
            System.out.println("Usage: " + DailyPriceAverageExample.class.getName() + " <path>");
            System.exit(1);
        }
        String journalLocation = args[0];
        // this is another way to setup JournalFactory if you would like to provide NullsAdaptor. NullsAdaptor for thrift,
        // which is used in this case implements JIT-friendly object reset method, which is quite fast.
        try (JournalFactory factory = new JournalFactory(new JournalConfiguration(new File(journalLocation)).setNullsAdaptorFactory(new ThriftNullsAdaptorFactory()).build())) {

            // delete existing quote journal
            Files.delete(new File(factory.getConfiguration().getJournalBase(), "quote"));

            int count = 10000000;
            long t = System.nanoTime();

            // get some data in :)
            try (JournalWriter<Quote> w = factory.writer(Quote.class)) {
                QuoteGenerator.generateQuoteData(w, count, 90);
            }

            System.out.println("Created " + count + " records in " + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t) + "ms");

            try (Journal<Quote> journal = factory.reader(Quote.class).setReadColumns("ask", "timestamp")) {
                count = 0;
                t = System.nanoTime();
                final String symbol = "BP.L";
                // create query builder to search for all records with key (sym) = "BP.L"
                final QueryAllBuilder<Quote> builder = journal.query().all().withKeys(symbol);
                final DateTimeField dayOfYear = ISOChronology.getInstanceUTC().dayOfYear();

                // state
                int previousDay = -1;
                double avgSum = 0;
                int avgCount = 0;

                try (JournalPrinter printer = new JournalPrinter()) {

                    // tell printer the types of objects we'll be producing
                    printer.types(String.class, DateTime.class, double.class);
                    // add fields to out output
                    // in this example we are using scalar values, so we have same number of fields as there are types.
                    // fields not declared here won't be printed.
                    printer.v(0).h("Symbol").v(1).h("Date").v(2).h("avg(Ask)");
                    // tell printer the appender we want to use, appender is anything implementing com.nfsdb.journal.printer.appender.Appender interface.
                    printer.setAppender(StdOutAppender.INSTANCE);

                    // print out header
                    printer.header();

                    // out result set is all chronologically ordered quotes for symbol BP.L
                    // so this loop leverages data order by printing out result when
                    // day of year changes
                    for (Quote q : builder.asResultSet().bufferedIterator()) {
                        int thisDay = dayOfYear.get(q.timestamp);
                        if (thisDay != previousDay) {
                            if (previousDay != -1) {
                                printer.out(symbol, Dates.utc().withDayOfYear(previousDay).withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0).withMillisOfSecond(0), avgSum / avgCount);
                            }
                            avgCount = 1;
                            avgSum = q.ask;
                            previousDay = thisDay;
                        } else {
                            avgCount++;
                            avgSum += q.ask;
                        }
                        count++;
                    }

                    if (previousDay != -1) {
                        printer.out(symbol, Dates.utc().withDayOfYear(previousDay).withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0).withMillisOfSecond(0), avgSum / avgCount);
                    }
                }
                System.out.println("Read " + count + " records in " + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t) + "ms");
            }
        }
    }
}
