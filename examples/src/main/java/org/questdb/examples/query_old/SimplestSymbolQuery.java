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

package org.questdb.examples.query_old;

import com.questdb.Journal;
import com.questdb.ex.JournalException;
import com.questdb.factory.JournalFactory;
import com.questdb.query.api.QueryAllBuilder;
import org.questdb.examples.model.ModelConfiguration;
import org.questdb.examples.model.Price;

import java.util.concurrent.TimeUnit;

public class SimplestSymbolQuery {
    public static void main(String[] args) throws JournalException {
        try (JournalFactory factory = new JournalFactory(ModelConfiguration.CONFIG.build(args[0]))) {
            try (Journal<Price> journal = factory.reader(Price.class)) {
                long tZero = System.nanoTime();
                int count = 0;
                QueryAllBuilder<Price> builder = journal.query().all().withSymValues("sym", "17");

                for (Price p : builder.asResultSet().bufferedIterator()) {
                    assert p != null;
                    count++;
                }

                System.out.println("Read " + count + " objects in " +
                        TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - tZero) + "ms.");
            }
        }
    }
}
