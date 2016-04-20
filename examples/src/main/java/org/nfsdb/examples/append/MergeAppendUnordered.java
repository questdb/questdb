/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (c) 2014-2016 Appsicle
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
 ******************************************************************************/

package org.nfsdb.examples.append;

import com.questdb.JournalWriter;
import com.questdb.ex.JournalException;
import com.questdb.factory.JournalFactory;
import com.questdb.factory.configuration.JournalConfigurationBuilder;
import com.questdb.misc.Files;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.nfsdb.examples.model.Quote;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@SuppressFBWarnings({"SACM_STATIC_ARRAY_CREATED_IN_METHOD", "PREDICTABLE_RANDOM"})
public class MergeAppendUnordered {


    /**
     * For cases where incoming data feed is not in chronological order but you would like your journal to be in chronological order.
     * This is a lossy way to append data as journal would only be merging a slice of data as specified by "lag" attribute.
     *
     * @param args factory directory
     * @throws JournalException
     */
    public static void main(String[] args) throws JournalException {

        if (args.length != 1) {
            System.out.println("Usage: " + MergeAppendUnordered.class.getName() + " <path>");
            System.exit(1);
        }

        String journalLocation = args[0];

        try (JournalFactory factory = new JournalFactory(new JournalConfigurationBuilder() {{
            $(Quote.class)
                    .location("quote-lag")
                    .lag(24, TimeUnit.HOURS) // enable lag
                    .$ts() // tell factory that Quote has "timestamp" column. If column is called differently you can pass its name
            ;
        }}.build(journalLocation))) {

            // delete existing quote journal
            Files.delete(new File(factory.getConfiguration().getJournalBase(), "quote-lag"));

            try (JournalWriter<Quote> writer = factory.writer(Quote.class)) {

                final String symbols[] = {"AGK.L", "BP.L", "TLW.L", "ABF.L", "LLOY.L", "BT-A.L", "WTB.L", "RRS.L", "ADM.L", "GKN.L", "HSBA.L"};
                final Random r = new Random(System.currentTimeMillis());


                // 20 batches of 50,000 quotes, total 1,000,000
                final int batchCount = 20;
                final int batchSize = 50000;
                final ArrayList<Quote> batch = new ArrayList<>(batchSize);
                // have pre-initialized array to reduce GC overhead
                for (int i = 0; i < batchSize; i++) {
                    batch.add(new Quote());
                }

                long utc = System.currentTimeMillis();
                long t = System.nanoTime();
                for (int i = 0; i < batchCount; i++) {

                    // populate batch in-memory
                    for (int k = 0; k < batchSize; k++) {
                        Quote q = batch.get(k);
                        q.clear();
                        // generate some data
                        q.setSym(symbols[Math.abs(r.nextInt() % (symbols.length - 1))]);
                        q.setAsk(Math.abs(r.nextDouble()));
                        q.setBid(Math.abs(r.nextDouble()));
                        q.setAskSize(Math.abs(r.nextInt() % 10000));
                        q.setBidSize(Math.abs(r.nextInt() % 10000));
                        q.setEx("LXE");
                        q.setMode("Fast trading");
                        long timestamp = utc + (i * batchSize + (batchSize - k)) * 1000L;
                        // make batches overlap (subtract 10 seconds)
                        timestamp -= 100000000L;
                        q.setTimestamp(timestamp);
                    }

                    // batch must be sorted before being presented to writer
                    Collections.sort(batch, writer.getTimestampComparator());

                    // append batch and have journal merge data
                    writer.mergeAppend(batch);
                }

                // commit is necessary
                writer.commit();
                System.out.println("Journal size: " + writer.size());
                System.out.println("Generated " + batchCount * batchSize + " objects in " + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t) + "ms.");
            }
        }
    }
}
