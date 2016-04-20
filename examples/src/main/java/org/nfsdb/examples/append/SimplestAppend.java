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
import com.questdb.misc.Files;
import org.nfsdb.examples.model.ModelConfiguration;
import org.nfsdb.examples.model.Price;

import java.io.File;
import java.util.concurrent.TimeUnit;

public class SimplestAppend {
    /**
     * Appends 1 million rows.
     *
     * @param args factory directory
     * @throws com.questdb.ex.JournalException
     */
    public static void main(String[] args) throws JournalException {
        try (JournalFactory factory = new JournalFactory(ModelConfiguration.CONFIG.build(args[0]))) {
            // delete existing price journal
            Files.delete(new File(factory.getConfiguration().getJournalBase(), Price.class.getName()));
            final int count = 1000000;

            try (JournalWriter<Price> writer = factory.writer(Price.class)) {
                long tZero = System.nanoTime();
                Price p = new Price();

                String symbols[] = new String[20];
                for (int i = 0; i < symbols.length; i++) {
                    symbols[i] = String.valueOf(i);
                }

                for (int i = 0; i < count; i++) {
                    p.setTimestamp(tZero + i);
                    p.setSym(symbols[i % 20]);
                    p.setPrice(i * 1.04598 + i);
                    writer.append(p);
                }

                // commit is necessary
                writer.commit();
                System.out.println("Persisted " + count + " objects in " +
                        TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - tZero) + "ms.");
            }
        }
    }
}
