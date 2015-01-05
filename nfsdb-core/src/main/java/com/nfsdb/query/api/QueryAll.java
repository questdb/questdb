/*
 * Copyright (c) 2014-2015. Vlad Ilyushchenko
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

package com.nfsdb.query.api;

import com.nfsdb.OrderedResultSet;
import com.nfsdb.exceptions.JournalException;
import com.nfsdb.iterators.ConcurrentIterator;
import com.nfsdb.iterators.JournalIterator;
import com.nfsdb.iterators.JournalPeekingIterator;
import com.nfsdb.iterators.JournalRowBufferedIterator;
import com.nfsdb.utils.Interval;

public interface QueryAll<T> extends Iterable<T> {

    OrderedResultSet<T> asResultSet() throws JournalException;

    long size();

    JournalPeekingIterator<T> bufferedIterator();

    JournalRowBufferedIterator<T> bufferedRowIterator();

    ConcurrentIterator<T> concurrentIterator();

    JournalIterator<T> iterator(Interval interval);

    QueryAllBuilder<T> withKeys(String... value);

    QueryAllBuilder<T> withSymValues(String symbol, String... value);

    JournalPeekingIterator<T> bufferedIterator(Interval interval);

    ConcurrentIterator<T> concurrentIterator(Interval interval);

    JournalPeekingIterator<T> iterator(long loRowID);

    JournalPeekingIterator<T> bufferedIterator(long rowid);

    JournalPeekingIterator<T> incrementBufferedIterator();

    JournalPeekingIterator<T> incrementIterator();

    ConcurrentIterator<T> concurrentIterator(long rowid);
}
