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
import com.nfsdb.journal.collections.IntArrayList;
import com.nfsdb.journal.lang.cst.*;
import com.nfsdb.journal.lang.cst.impl.dsrc.DataSourceImpl;
import com.nfsdb.journal.lang.cst.impl.fltr.*;
import com.nfsdb.journal.lang.cst.impl.join.SymbolOuterJoin;
import com.nfsdb.journal.lang.cst.impl.jsrc.JournalSourceImpl;
import com.nfsdb.journal.lang.cst.impl.jsrc.TopJournalSource;
import com.nfsdb.journal.lang.cst.impl.ksrc.*;
import com.nfsdb.journal.lang.cst.impl.psrc.IntervalPartitionSource;
import com.nfsdb.journal.lang.cst.impl.psrc.JournalDescPartitionSource;
import com.nfsdb.journal.lang.cst.impl.psrc.JournalPartitionSource;
import com.nfsdb.journal.lang.cst.impl.psrc.JournalTailPartitionSource;
import com.nfsdb.journal.lang.cst.impl.ref.IntRef;
import com.nfsdb.journal.lang.cst.impl.ref.StringRef;
import com.nfsdb.journal.lang.cst.impl.rsrc.*;
import org.joda.time.Interval;

import java.util.List;

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
    public RowSource top(int count, RowSource rowSource) {
        return new TopRowSource(count, rowSource);
    }

    @Override
    public JournalSource top(int count, JournalSource source) {
        return new TopJournalSource(count, source);
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
    public RowSource kvSource(StringRef indexName, KeySource keySource) {
        return new KvIndexRowSource(indexName, keySource);
    }

    @Override
    public RowSource headEquals(StringRef column, StringRef value) {
        return kvSource(column, hashSource(column, value), 1, 0, equalsConst(column, value));
    }

    public RowSource headEquals(StringRef column, IntRef value) {
        return kvSource(column, new SingleIntHashKeySource(column, value), 1, 0, new IntEqualsRowFilter(column, value));
    }

    @Override
    public RowSource all() {
        return new AllRowSource();
    }

    @Override
    public RowSource kvSource(StringRef indexName, KeySource keySource, int count, int tail, RowFilter filter) {
        return new KvIndexHeadRowSource(indexName, keySource, count, tail, filter);
    }

    @Override
    public PartitionSource source(Journal journal, boolean open) {
        return new JournalPartitionSource(journal, open);
    }

    @Override
    public PartitionSource sourceDesc(Journal journal, boolean open) {
        return new JournalDescPartitionSource(journal, open);
    }

    @Override
    public PartitionSource sourceDesc(Journal journal) {
        return new JournalDescPartitionSource(journal, false);
    }

    @Override
    public PartitionSource source(Journal journal, boolean open, long rowid) {
        return new JournalTailPartitionSource(journal, open, rowid);
    }

    @Override
    public RowFilter equalsConst(StringRef column, StringRef value) {
        return new StringEqualsRowFilter(column, value);
    }

    @Override
    public RowFilter equalsSymbol(StringRef column, StringRef value) {
        return new SymbolEqualsRowFilter(column, value);
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
    public KeySource symbolTableSource(StringRef sym, List<String> values) {
        return new PartialSymbolKeySource(sym, values);
    }

    @Override
    public KeySource singleKeySource(IntRef key) {
        return new SingleKeySource(key);
    }

    @Override
    public KeySource symbolTableSource(StringRef sym) {
        return new SymbolKeySource(sym);
    }

    @Override
    public KeySource hashSource(StringRef column, List<String> value) {
        return new StringHashKeySource(column, value);
    }

    @Override
    public KeySource hashSource(StringRef column, StringRef value) {
        return new SingleStringHashKeySource(column, value);
    }

    public KeySource hashSource(StringRef column, IntArrayList values) {
        return new IntHashKeySource(column, values);
    }

    @Override
    public JoinedSource join(JournalSource masterSource, StringRef masterSymbol, IntRef keyRef, JournalSource slaveSource, StringRef slaveSymbol, RowFilter filter) {
        return new SymbolOuterJoin(masterSource, masterSymbol, keyRef, slaveSource, slaveSymbol);
    }

    @Override
    public JournalSourceLookup lastNKeyLookup(String column, int n, PartitionSource partitionSource) {
        return null;
    }
}
