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

package com.nfsdb.journal.index.experimental.v2;

import com.nfsdb.journal.Journal;
import org.joda.time.Interval;

public interface Q {
    PartitionSource interval(PartitionSource iterator, Interval interval);

    JournalSource forEachPartition(PartitionSource iterator, RowSource source);

    RowSource forEachRow(RowSource source, RowFilter rowFilter);

    RowSource forEachKv(String indexName, KvFilterSource filterSource);

    RowSource union(RowSource... source);

    RowSource mergeSorted(RowSource source1, RowSource source2);

    RowSource join(RowSource source1, RowSource source2);

    RowSource kvSource(String indexName, KeySource keySource);

    PartitionSource source(Journal journal);

    RowFilter equalsConst(String column, String value);

    RowFilter equals(String columnA, String columnB);

    RowFilter equals(String column, int value);

    RowFilter greaterThan(String column, double value);

    RowFilter all(RowFilter... rowFilter);

    RowFilter any(RowFilter... rowFilters);

    RowFilter not(RowFilter rowFilter);

    KeySource symbolTableSource(String sym, String... values);

    KeySource hashSource(String... value);

    /**
     * On first partition sources all keys from keySource.
     * As KvFilter accepts only first N rows per key. Because rows are fed in last-to-first, first N for KvFilter means last N chronologically.
     * All keys with -1 rowID are re-sourced for next partition.
     *
     * @param keySource the KeySource for first partition
     * @param n         number of rows per key value to accept.
     * @return KvFilterSource instance for wiring.
     */
    KvFilterSource lastNGroupByKey(KeySource keySource, int n);

    /**
     * On first partition sources all keys from keySource.
     * As KvFilter accepts only first N rows per key that match filter. Because rows are fed in last-to-first, first N for KvFilter means last N chronologically.
     * All keys with -1 rowID are re-sourced for next partition.
     *
     * @param keySource the KeySource for first partition
     * @param n         number of rows per key value to accept.
     * @return KvFilterSource instance for wiring.
     */
    KvFilterSource lastNGroupByKey(KeySource keySource, int n, RowFilter filter);

    JoinedSource join(String column, JournalSource masterSource, JournalSourceLookup lookupSource, RowFilter filter);

    JournalSourceLookup lastNKeyLookup(String column, int n, PartitionSource partitionSource);
}
