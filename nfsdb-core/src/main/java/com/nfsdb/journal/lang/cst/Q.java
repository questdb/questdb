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

package com.nfsdb.journal.lang.cst;

import com.nfsdb.journal.Journal;
import org.joda.time.Interval;

public interface Q {

    <T> DataSource<T> ds(JournalSource journalSource, T instance);

    PartitionSource interval(PartitionSource iterator, Interval interval);

    JournalSource forEachPartition(PartitionSource iterator, RowSource source);

    RowSource forEachRow(RowSource source, RowFilter rowFilter);

    RowSource top(int count, RowSource rowSource);

    JournalSource top(int count, JournalSource source);

    RowSource union(RowSource... source);

    RowSource mergeSorted(RowSource source1, RowSource source2);

    RowSource join(RowSource source1, RowSource source2);

    RowSource kvSource(String indexName, KeySource keySource);

    RowSource kvSource(String indexName, KeySource keySource, int count, int tail, RowFilter filter);

    PartitionSource source(Journal journal, boolean open);

    PartitionSource source(Journal journal, boolean open, long rowid);

    RowFilter equalsConst(String column, String value);

    RowFilter equals(String columnA, String columnB);

    RowFilter equals(String column, int value);

    RowFilter greaterThan(String column, double value);

    RowFilter all(RowFilter... rowFilter);

    RowFilter any(RowFilter... rowFilters);

    RowFilter not(RowFilter rowFilter);

    KeySource symbolTableSource(String sym, String... values);

    KeySource symbolTableSource(String sym);

    KeySource hashSource(String column, String... value);

    JoinedSource join(String column, JournalSource masterSource, JournalSourceLookup lookupSource, RowFilter filter);

    JournalSourceLookup lastNKeyLookup(String column, int n, PartitionSource partitionSource);
}
