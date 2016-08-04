/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2016 Appsicle
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

package org.questdb.examples.query;

import com.questdb.JournalEntryWriter;
import com.questdb.JournalWriter;
import com.questdb.ex.JournalException;
import com.questdb.ex.ParserException;
import com.questdb.factory.JournalFactory;
import com.questdb.factory.configuration.JournalStructure;
import com.questdb.misc.Files;
import com.questdb.misc.Rnd;
import com.questdb.ql.Record;
import com.questdb.ql.RecordCursor;
import com.questdb.ql.RecordSource;
import com.questdb.ql.parser.QueryCompiler;

import java.io.File;

public class GenericAppend {

    public static void createCustomers(JournalFactory factory) throws JournalException {
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

    public static void main(String[] args) throws JournalException, ParserException {
        if (args.length < 1) {
            System.out.println("Usage: GenericAppend <path>");
            System.exit(1);
        }

        final String location = args[0];

        deleteCustomers(location);

        // factory can be reused in application and must be explicitly closed when no longer needed.
        try (JournalFactory factory = new JournalFactory(location)) {

            createCustomers(factory);

            // Query compile instance can and must be reused.

            QueryCompiler compiler = new QueryCompiler();

            try (RecordSource rs = compiler.compile(factory, "customers")) {
                RecordCursor cursor = rs.prepareCursor(factory);
                System.out.println(fetchCursor(cursor) + " records selected");
            }
        }
    }
}
