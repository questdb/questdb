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

package org.questdb.examples.support;

import com.questdb.std.Rnd;
import com.questdb.std.ex.JournalException;
import com.questdb.store.*;
import com.questdb.store.factory.WriterFactory;
import com.questdb.store.factory.configuration.JournalStructure;

import java.io.File;
import java.util.Random;

public class Generator {
    public static void createCustomers(WriterFactory factory) throws JournalException {
        // Lets add some random data to journal "customers".
        // This journal does not have associated java object. We will leverage generic data access
        // to populate it.
        try (JournalWriter writer = factory.writer(
                new JournalStructure("customers").
                        $int("id").
                        $str("name").
                        $ts("updateDate").
                        $())) {

            Rnd rnd = new Rnd();

            int updateDateIndex = writer.getMetadata().getColumnIndex("updateDate");

            long timestamp = System.currentTimeMillis();
            for (int i = 0; i < 1000000; i++) {
                // timestamp order is enforced by passing value to entryWriter() call
                // in this example we don't pass timestamp and ordering is not enforced
                JournalEntryWriter ew = writer.entryWriter();

                // columns accessed by index
                ew.putInt(0, rnd.nextPositiveInt());
                ew.putStr(1, rnd.nextChars(25));

                // you can use column index we looked up earlier
                ew.putDate(updateDateIndex, timestamp);

                // increment timestamp by 30 seconds
                timestamp += 30000;

                // append record to journal
                ew.append();
            }
            // commit all records at once
            // there is no limit on how many records can be in the same transaction
            writer.commit();
        }
    }

    public static void deleteCustomers(String location) {
        // allow this example to re-run
        Files.delete(new File(location, "customers"));
    }

    public static int fetchCursor(RecordCursor cursor) {
        int count = 0;
        while (cursor.hasNext()) {
            Record r = cursor.next();

            r.getInt(0);
            r.getFlyweightStr(1);
            r.getDate(2);

            count++;
        }
        return count;
    }

    public static void generateQuoteData(JournalWriter<Quote> w, int count) throws JournalException {
        generateQuoteData(w, count, 30);
    }

    public static void generateQuoteData(JournalWriter<Quote> w, int count, int days) throws JournalException {
        String symbols[] = {"AGK.L", "BP.L", "TLW.L", "ABF.L", "LLOY.L", "BT-A.L", "WTB.L", "RRS.L", "ADM.L", "GKN.L", "HSBA.L"};
        generateQuoteData(w, count, days, symbols);
    }

    public static void generateQuoteData(JournalWriter<Quote> w, int count, int days, String[] symbols) throws JournalException {
        long lo = System.currentTimeMillis();
        long hi = lo + ((long) days) * 24 * 60 * 60 * 1000L;
        long delta = (hi - lo) / count;

        Quote q = new Quote();
        Random r = new Random(System.currentTimeMillis());
        for (int i = 0; i < count; i++) {
            q.clear();
            q.setSym(symbols[Math.abs(r.nextInt() % (symbols.length))]);
            q.setAsk(Math.abs(r.nextDouble()));
            q.setBid(Math.abs(r.nextDouble()));
            q.setAskSize(Math.abs(r.nextInt() % 10000));
            q.setBidSize(Math.abs(r.nextInt() % 10000));
            q.setEx("LXE");
            q.setMode("Fast trading");
            // timestamp must always be in milliseconds
            q.setTimestamp(lo);
            w.append(q);
            lo += delta;
        }
        w.commit();
    }

    public static String randomString(Random random, int len) {
        char chars[] = new char[len];
        for (int i = 0; i < len; i++) {
            chars[i] = (char) (Math.abs(random.nextInt() % 25) + 66);
        }
        return new String(chars);
    }

    public static String[] randomSymbols(int count) {
        Random r = new Random(System.currentTimeMillis());
        String[] result = new String[count];
        for (int i = 0; i < result.length; i++) {
            result[i] = randomString(r, 4) + ".L";
        }
        return result;
    }
}
