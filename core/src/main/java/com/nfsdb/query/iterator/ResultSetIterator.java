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

package com.nfsdb.query.iterator;

import com.nfsdb.Journal;
import com.nfsdb.ex.JournalException;
import com.nfsdb.ex.JournalRuntimeException;
import com.nfsdb.iter.JournalIterator;
import com.nfsdb.iter.PeekingIterator;
import com.nfsdb.query.ResultSet;
import com.nfsdb.std.AbstractImmutableIterator;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings({"EXS_EXCEPTION_SOFTENING_NO_CHECKED", "CD_CIRCULAR_DEPENDENCY"})
public class ResultSetIterator<T> extends AbstractImmutableIterator<T> implements JournalIterator<T>, PeekingIterator<T> {

    private final ResultSet<T> rs;
    private int cursor = 0;

    public ResultSetIterator(ResultSet<T> rs) {
        this.rs = rs;
    }

    @Override
    public Journal<T> getJournal() {
        return rs.getJournal();
    }

    @Override
    public boolean hasNext() {
        return cursor < rs.size();
    }

    @Override
    public T next() {
        try {
            return rs.read(cursor++);
        } catch (JournalException e) {
            throw new JournalRuntimeException("Journal exception", e);
        }
    }

    @Override
    public boolean isEmpty() {
        return cursor >= rs.size();
    }

    @Override
    public T peekFirst() {
        try {
            return rs.readFirst();
        } catch (JournalException e) {
            throw new JournalRuntimeException("Journal exception", e);
        }
    }

    @Override
    public T peekLast() {
        try {
            return rs.readLast();
        } catch (JournalException e) {
            throw new JournalRuntimeException("Journal exception", e);
        }
    }
}
