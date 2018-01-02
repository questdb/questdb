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

package com.questdb.store.query.spi;

import com.questdb.store.Journal;
import com.questdb.store.query.api.Query;
import com.questdb.store.query.api.QueryAll;
import com.questdb.store.query.api.QueryHead;

public class QueryImpl<T> implements Query<T> {

    private final Journal<T> journal;
    private final QueryAllImpl<T> allImpl;
    private final QueryHeadImpl<T> headImpl;

    public QueryImpl(Journal<T> journal) {
        this.journal = journal;
        this.allImpl = new QueryAllImpl<>(journal);
        this.headImpl = new QueryHeadImpl<>(journal);
    }

    @Override
    public QueryAll<T> all() {
        return allImpl;
    }

    @Override
    public Journal<T> getJournal() {
        return journal;
    }

    @Override
    public QueryHead<T> head() {
        return headImpl;
    }
}
