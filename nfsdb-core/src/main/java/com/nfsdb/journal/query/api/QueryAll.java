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

package com.nfsdb.journal.query.api;

import com.nfsdb.journal.OrderedResultSet;
import com.nfsdb.journal.exceptions.JournalException;
import com.nfsdb.journal.iterators.ConcurrentIterator;
import com.nfsdb.journal.iterators.JournalIterator;
import com.nfsdb.journal.iterators.JournalRowBufferedIterator;
import org.joda.time.Interval;

public interface QueryAll<T> extends Iterable<T> {

    OrderedResultSet<T> asResultSet() throws JournalException;

    long size();

    JournalIterator<T> bufferedIterator();

    JournalRowBufferedIterator<T> bufferedRowIterator();

    ConcurrentIterator<T> concurrentIterator();

    JournalIterator<T> iterator(Interval interval);

    QueryAllBuilder<T> withKeys(String... value);

    QueryAllBuilder<T> withSymValues(String symbol, String... value);

    JournalIterator<T> bufferedIterator(Interval interval);

    ConcurrentIterator<T> concurrentIterator(Interval interval);

    JournalIterator<T> iterator(long loRowID);

    JournalIterator<T> bufferedIterator(long rowid);

    JournalIterator<T> incrementBufferedIterator();

    JournalIterator<T> incrementIterator();

    ConcurrentIterator<T> concurrentIterator(long rowid);
}
