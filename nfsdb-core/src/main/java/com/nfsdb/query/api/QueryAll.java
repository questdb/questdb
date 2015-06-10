/*******************************************************************************
 *   _  _ ___ ___     _ _
 *  | \| | __/ __| __| | |__
 *  | .` | _|\__ \/ _` | '_ \
 *  |_|\_|_| |___/\__,_|_.__/
 *
 *  Copyright (c) 2014-2015. The NFSdb project and its contributors.
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
 ******************************************************************************/
package com.nfsdb.query.api;

import com.nfsdb.exceptions.JournalException;
import com.nfsdb.query.OrderedResultSet;
import com.nfsdb.query.iterator.ConcurrentIterator;
import com.nfsdb.query.iterator.JournalIterator;
import com.nfsdb.query.iterator.JournalPeekingIterator;
import com.nfsdb.utils.Interval;

public interface QueryAll<T> extends Iterable<T> {

    OrderedResultSet<T> asResultSet() throws JournalException;

    JournalPeekingIterator<T> bufferedIterator();

    JournalPeekingIterator<T> bufferedIterator(Interval interval);

    JournalPeekingIterator<T> bufferedIterator(long rowid);

    ConcurrentIterator<T> concurrentIterator();

    ConcurrentIterator<T> concurrentIterator(Interval interval);

    ConcurrentIterator<T> concurrentIterator(long rowid);

    JournalPeekingIterator<T> incrementBufferedIterator();

    JournalPeekingIterator<T> incrementIterator();

    JournalIterator<T> iterator(Interval interval);

    JournalPeekingIterator<T> iterator(long loRowID);

    long size();

    QueryAllBuilder<T> withKeys(String... value);

    QueryAllBuilder<T> withSymValues(String symbol, String... value);
}
