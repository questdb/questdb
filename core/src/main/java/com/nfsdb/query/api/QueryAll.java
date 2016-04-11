/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (C) 2014-2016 Appsicle
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
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 *
 ******************************************************************************/

package com.nfsdb.query.api;

import com.nfsdb.ex.JournalException;
import com.nfsdb.iter.ConcurrentIterator;
import com.nfsdb.iter.JournalIterator;
import com.nfsdb.iter.JournalPeekingIterator;
import com.nfsdb.misc.Interval;
import com.nfsdb.query.OrderedResultSet;

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
