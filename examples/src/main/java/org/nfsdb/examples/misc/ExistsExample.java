/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
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

package org.nfsdb.examples.misc;

import com.nfsdb.Journal;
import com.nfsdb.JournalWriter;
import com.nfsdb.PartitionType;
import com.nfsdb.ex.JournalException;
import com.nfsdb.factory.JournalFactory;
import com.nfsdb.factory.configuration.JournalConfigurationBuilder;
import com.nfsdb.misc.Files;
import com.nfsdb.store.SymbolTable;
import org.nfsdb.examples.model.Quote;
import org.nfsdb.examples.support.QuoteGenerator;

import java.io.File;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class ExistsExample {

    public static void main(String[] args) throws JournalException {
        if (args.length != 1) {
            System.out.println("Usage: " + ExistsExample.class.getName() + " <path>");
            System.exit(1);
        }
        try (JournalFactory factory = new JournalFactory(new JournalConfigurationBuilder() {{
            $(Quote.class)
                    .partitionBy(PartitionType.MONTH)
                    .$sym("sym").index().size(4).valueCountHint(15)
                    .$sym("ex").size(2).valueCountHint(1)
                    .$sym("ex").size(1).valueCountHint(1)
                    .$ts()
            ;
        }}.build(args[0]))) {

            // delete existing quote journal
            Files.delete(new File(factory.getConfiguration().getJournalBase(), Quote.class.getName()));

            // get some data in :)
            try (JournalWriter<Quote> w = factory.writer(Quote.class)) {
                QuoteGenerator.generateQuoteData(w, 1000000);
            }

            final Set<String> values = new HashSet<String>() {{
                add("TLW.L");
                add("ABF.L");
                add("LLOY.L");
                add("TLZ.L");
                add("BT-A.L");
                add("KBR.L");
                add("WTB.L");
            }};

            try (Journal<Quote> journal = factory.reader(Quote.class)) {
                long t = System.nanoTime();
                //
                // check values against SymbolTable, if they are there they would exist in journal too.
                //
                SymbolTable tab = journal.getSymbolTable("sym");
                for (String v : values) {
                    if (tab.getQuick(v) == SymbolTable.VALUE_NOT_FOUND) {
                        System.out.println(v + ": MISSING");
                    } else {
                        System.out.println(v + ": ok");
                    }
                }
                System.out.println("Done in " + TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - t) + "μs");
            }
        }
    }
}
