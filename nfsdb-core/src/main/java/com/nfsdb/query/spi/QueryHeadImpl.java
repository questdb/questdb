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

package com.nfsdb.query.spi;

import com.nfsdb.Journal;
import com.nfsdb.exceptions.JournalException;
import com.nfsdb.map.JournalHashMap;
import com.nfsdb.map.JournalMap;
import com.nfsdb.query.api.QueryHead;
import com.nfsdb.query.api.QueryHeadBuilder;

public class QueryHeadImpl<T> implements QueryHead<T> {
    private final Journal<T> journal;

    public QueryHeadImpl(Journal<T> journal) {
        this.journal = journal;
    }

    @Override
    public QueryHeadBuilder<T> withKeys(String... values) {
        return withSymValues(journal.getMetadata().getKey(), values);
    }

    @Override
    public QueryHeadBuilder<T> withSymValues(String symbol, String... values) {
        QueryHeadBuilderImpl<T> impl = new QueryHeadBuilderImpl<>(journal);
        impl.setSymbol(symbol, values);
        return impl;
    }

    @Override
    public JournalMap<T> map() throws JournalException {
        return new JournalHashMap<>(this.journal).eager();
    }
}
