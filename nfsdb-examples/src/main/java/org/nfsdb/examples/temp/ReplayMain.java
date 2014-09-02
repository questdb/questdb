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

package org.nfsdb.examples.temp;

import com.nfsdb.journal.Journal;
import com.nfsdb.journal.exceptions.JournalException;
import com.nfsdb.journal.factory.JournalFactory;
import com.nfsdb.journal.iterators.ReplayIterator;
import com.nfsdb.journal.iterators.TickSource;
import com.nfsdb.journal.iterators.clock.MilliClock;
import org.nfsdb.examples.model.Quote;

public class ReplayMain {
    public static void main(String[] args) throws JournalException {

        JournalFactory factory = new JournalFactory("/path");
        Journal<Quote> reader = factory.reader(Quote.class);

        ReplayIterator<Quote> replay = new ReplayIterator<>(reader, MilliClock.INSTANCE, 1f, new TickSource<Quote>() {
            @Override
            public long getTicks(Quote object) {
                return object.getTimestamp();
            }
        });

        for (Quote q : replay) {
            //
        }
    }
}
