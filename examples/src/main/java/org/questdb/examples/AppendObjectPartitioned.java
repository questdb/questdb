/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2018 Appsicle
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
 *
 ******************************************************************************/

package org.questdb.examples;

import com.questdb.std.Rnd;
import com.questdb.std.ex.JournalException;
import com.questdb.store.Files;
import com.questdb.store.JournalWriter;
import com.questdb.store.PartitionBy;
import com.questdb.store.factory.Factory;
import com.questdb.store.factory.configuration.JournalConfigurationBuilder;
import org.questdb.examples.support.Quote;

import java.io.File;
import java.util.concurrent.TimeUnit;

public class AppendObjectPartitioned {

    /**
     * Appends 1 million quotes into journal partitioned by day. Journal can only be partitioned on values of timestamp column.
     *
     * @param args factory directory
     * @throws JournalException in case of any problems with the journal.
     */
    public static void main(String[] args) throws JournalException {

        if (args.length != 1) {
            System.out.println("Usage: " + AppendObjectPartitioned.class.getName() + " <path>");
            System.exit(1);
        }

        String journalLocation = args[0];
        try (Factory factory = new Factory(new JournalConfigurationBuilder() {{
            $(Quote.class, "quote-by-day")
                    // partition by day
                    .partitionBy(PartitionBy.DAY)
                    // tell factory that Quote has "timestamp" column. If column is called differently you can pass its name
                    .$ts()
            ;
        }}.build(journalLocation), 1000, 1, 0)) {

            Files.delete(new File(factory.getConfiguration().getJournalBase(), "quote-by-day"));

            // default partition type for Quote is MONTH (see ModelConfiguration)
            // you can change it in runtime and also, optionally put journal in alternative location
            try (JournalWriter<Quote> writer = factory.writer(Quote.class)) {

                final int count = 1000000;
                final String symbols[] = {"AGK.L", "BP.L", "TLW.L", "ABF.L", "LLOY.L", "BT-A.L", "WTB.L", "RRS.L", "ADM.L", "GKN.L", "HSBA.L"};
                final Rnd r = new Rnd();

                // reuse same same instance of Quote class to keep GC under control
                final Quote q = new Quote();

                long t = System.nanoTime();
                for (int i = 0; i < count; i++) {
                    // prepare object for new set of data
                    q.clear();
                    // generate some data
                    q.setSym(symbols[Math.abs(r.nextInt() % (symbols.length - 1))]);
                    q.setAsk(Math.abs(r.nextDouble()));
                    q.setBid(Math.abs(r.nextDouble()));
                    q.setAskSize(Math.abs(r.nextInt() % 10000));
                    q.setBidSize(Math.abs(r.nextInt() % 10000));
                    q.setEx("LXE");
                    q.setMode("Fast trading");
                    q.setTimestamp(System.currentTimeMillis() + (i * 100));
                    writer.append(q);
                }

                // commit is necessary
                writer.commit();
                System.out.println("Journal size: " + writer.size());
                System.out.println("Generated " + count + " objects in " + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t) + "ms.");
            }
        }
    }
}
