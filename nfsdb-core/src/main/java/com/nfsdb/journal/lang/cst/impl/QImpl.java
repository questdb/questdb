package com.nfsdb.journal.lang.cst.impl;

import com.nfsdb.journal.Journal;
import com.nfsdb.journal.lang.cst.*;
import org.joda.time.Interval;

public class QImpl implements Q{
    @Override
    public PartitionSource interval(PartitionSource iterator, Interval interval) {
        return new PartitionSourceIntervalImpl(iterator, interval);
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
    public RowSource forEachKv(String indexName, KvFilterSource filterSource) {
        return null;
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
    public PartitionSource source(Journal journal, boolean open) {
        return new PartitionSourceImpl(journal, open);
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
        return new GreaterThanRowFilter(column, value);
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
        return new SymbolTableKeySourceImpl(sym, values);
    }

    @Override
    public KeySource hashSource(String column, String... value) {
        return new HashKeySource(column, value);
    }

    @Override
    public KvFilterSource lastNGroupByKey(KeySource keySource, int n) {
        return null;
    }

    @Override
    public KvFilterSource lastNGroupByKey(KeySource keySource, int n, RowFilter filter) {
        return null;
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
