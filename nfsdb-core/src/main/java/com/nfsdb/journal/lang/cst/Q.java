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
import com.nfsdb.journal.lang.cst.impl.ref.IntRef;
import com.nfsdb.journal.lang.cst.impl.ref.StringRef;
import org.joda.time.Interval;

import java.util.List;

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

    RowSource kvSource(StringRef indexName, KeySource keySource);

    RowSource headEquals(StringRef column, StringRef value);

    RowSource headEquals(StringRef column, IntRef value);

    RowSource all();

    RowSource kvSource(StringRef indexName, KeySource keySource, int count, int tail, RowFilter filter);

    PartitionSource source(Journal journal, boolean open);

    PartitionSource sourceDesc(Journal journal, boolean open);

    PartitionSource sourceDesc(Journal journal);

    PartitionSource source(Journal journal, boolean open, long rowid);

    RowFilter equalsConst(StringRef column, StringRef value);

    RowFilter equalsSymbol(StringRef column, StringRef value);

    RowFilter equals(String columnA, String columnB);

    RowFilter equals(String column, int value);

    RowFilter greaterThan(String column, double value);

    RowFilter all(RowFilter... rowFilter);

    RowFilter any(RowFilter... rowFilters);

    RowFilter not(RowFilter rowFilter);

    KeySource symbolTableSource(StringRef sym, List<String> values);

    KeySource singleKeySource(IntRef key);

    KeySource symbolTableSource(StringRef sym);

    KeySource hashSource(StringRef column, List<String> values);

    KeySource hashSource(StringRef column, StringRef value);

    JoinedSource join(JournalSource masterSource, StringRef masterSymbol, IntRef keyRef, JournalSource slaveSource, StringRef slaveSymbol, RowFilter filter);

    JournalSourceLookup lastNKeyLookup(String column, int n, PartitionSource partitionSource);
}
