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

package com.nfsdb.journal.lang.cst.impl;

import com.nfsdb.journal.Journal;
import com.nfsdb.journal.lang.cst.*;
import com.nfsdb.journal.lang.cst.impl.fltr.AllRowFilter;
import com.nfsdb.journal.lang.cst.impl.fltr.DoubleGreaterThanRowFilter;
import com.nfsdb.journal.lang.cst.impl.fltr.StringEqualsRowFilter;
import com.nfsdb.journal.lang.cst.impl.ksrc.PartialSymbolKeySource;
import com.nfsdb.journal.lang.cst.impl.ksrc.StringHashKeySource;
import com.nfsdb.journal.lang.cst.impl.ksrc.SymbolKeySource;
import com.nfsdb.journal.lang.cst.impl.psrc.IntervalPartitionSource;
import com.nfsdb.journal.lang.cst.impl.psrc.JournalPartitionSource;
import com.nfsdb.journal.lang.cst.impl.psrc.JournalTailPartitionSource;
import com.nfsdb.journal.lang.cst.impl.rsrc.FilteredRowSource;
import com.nfsdb.journal.lang.cst.impl.rsrc.KvIndexRowSource;
import com.nfsdb.journal.lang.cst.impl.rsrc.KvIndexTailRowSource;
import com.nfsdb.journal.lang.cst.impl.rsrc.UnionRowSource;
import org.joda.time.Interval;

public class QImpl implements Q {

    @Override
    public <T> DataSource<T> ds(JournalSource journalSource, T instance) {
        return new DataSourceImpl<>(journalSource, instance);
    }

    @Override
    public PartitionSource interval(PartitionSource iterator, Interval interval) {
        return new IntervalPartitionSource(iterator, interval);
    }

    @Override
    public JournalSource forEachPartition(PartitionSource iterator, RowSource source) {
        return new JournalSourceImpl(iterator, source);
    }

    @Override
    public RowSource forEachRow(RowSource source, RowFilter rowFilter) {
        return new FilteredRowSource(source, rowFilter);
    }

    @Override
    public RowSource union(RowSource... source) {
        return new UnionRowSource(source);
    }

    @Override
    public RowSource mergeSorted(RowSource source1, RowSource source2) {
        return null;
    }

    @Override
    public RowSource join(RowSource source1, RowSource source2) {
        return null;
    }

    @Override
    public RowSource kvSource(String indexName, KeySource keySource) {
        return new KvIndexRowSource(indexName, keySource);
    }

    @Override
    public RowSource kvSource(String indexName, KeySource keySource, int count, int tail, RowFilter filter) {
        return new KvIndexTailRowSource(indexName, keySource, count, tail, filter);
    }

    @Override
    public PartitionSource source(Journal journal, boolean open) {
        return new JournalPartitionSource(journal, open);
    }

    @Override
    public PartitionSource source(Journal journal, boolean open, long rowid) {
        return new JournalTailPartitionSource(journal, open, rowid);
    }

    @Override
    public RowFilter equalsConst(String column, String value) {
        return new StringEqualsRowFilter(column, value);
    }

    @Override
    public RowFilter equals(String columnA, String columnB) {
        return null;
    }

    @Override
    public RowFilter equals(String column, int value) {
        return null;
    }

    @Override
    public RowFilter greaterThan(String column, double value) {
        return new DoubleGreaterThanRowFilter(column, value);
    }

    @Override
    public RowFilter all(RowFilter... rowFilter) {
        return new AllRowFilter(rowFilter);
    }

    @Override
    public RowFilter any(RowFilter... rowFilters) {
        return null;
    }

    @Override
    public RowFilter not(RowFilter rowFilter) {
        return null;
    }

    @Override
    public KeySource symbolTableSource(String sym, String... values) {
        return new PartialSymbolKeySource(sym, values);
    }

    @Override
    public KeySource symbolTableSource(String sym) {
        return new SymbolKeySource(sym);
    }

    @Override
    public KeySource hashSource(String column, String... value) {
        return new StringHashKeySource(column, value);
    }

    @Override
    public JoinedSource join(String column, JournalSource masterSource, JournalSourceLookup lookupSource, RowFilter filter) {
        return null;
    }

    @Override
    public JournalSourceLookup lastNKeyLookup(String column, int n, PartitionSource partitionSource) {
        return null;
    }
}
