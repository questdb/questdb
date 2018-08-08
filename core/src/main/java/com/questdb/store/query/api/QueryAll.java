/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.store.query.api;

import com.questdb.std.ex.JournalException;
import com.questdb.store.Interval;
import com.questdb.store.query.OrderedResultSet;
import com.questdb.store.query.iter.ConcurrentIterator;
import com.questdb.store.query.iter.JournalIterator;
import com.questdb.store.query.iter.JournalPeekingIterator;

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
