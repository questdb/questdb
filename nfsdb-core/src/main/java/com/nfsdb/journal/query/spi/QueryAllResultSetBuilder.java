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

package com.nfsdb.journal.query.spi;

import com.nfsdb.journal.Partition;
import com.nfsdb.journal.UnorderedResultSetBuilder;
import com.nfsdb.journal.collections.IntArrayList;
import com.nfsdb.journal.collections.LongArrayList;
import com.nfsdb.journal.exceptions.JournalException;
import com.nfsdb.journal.index.Cursor;
import com.nfsdb.journal.index.KVIndex;
import com.nfsdb.journal.utils.Rows;
import org.joda.time.Interval;

import java.util.List;

public class QueryAllResultSetBuilder<T> extends UnorderedResultSetBuilder<T> {
    private final IntArrayList symbolKeys;
    private final List<String> filterSymbols;
    private final IntArrayList filterSymbolKeys;
    final private String symbol;
    private KVIndex index;
    private KVIndex[] searchIndices;

    public QueryAllResultSetBuilder(Interval interval, String symbol, IntArrayList symbolKeys, List<String> filterSymbols, IntArrayList filterSymbolKeys) {
        super(interval);
        this.symbol = symbol;
        this.symbolKeys = symbolKeys;
        this.filterSymbols = filterSymbols;
        this.filterSymbolKeys = filterSymbolKeys;
    }

    @Override
    public Accept accept(Partition<T> partition) throws JournalException {
        super.accept(partition);
        this.index = partition.open().getIndexForColumn(symbol);

        // check if partition has at least one symbol value
        if (symbolKeys.size() > 0) {
            for (int i = 0; i < symbolKeys.size(); i++) {
                if (index.contains(symbolKeys.getQuick(i))) {
                    searchIndices = new KVIndex[filterSymbols.size()];
                    for (int k = 0; k < filterSymbols.size(); k++) {
                        searchIndices[k] = partition.getIndexForColumn(filterSymbols.get(k));
                    }
                    return Accept.CONTINUE;
                }
            }
            return Accept.SKIP;
        }
        return Accept.BREAK;
    }

    @Override
    public void read(long lo, long hi) {
        for (int i = 0; i < symbolKeys.size(); i++) {
            int symbolKey = symbolKeys.getQuick(i);
            if (index.contains(symbolKey)) {
                if (searchIndices.length > 0) {
                    for (int k = 0; k < searchIndices.length; k++) {
                        if (searchIndices[k].contains(filterSymbolKeys.get(k))) {
                            LongArrayList searchLocalRowIDs = searchIndices[k].getValues(filterSymbolKeys.get(k));

                            Cursor cursor = index.cachedCursor(symbolKey);
                            while (cursor.hasNext()) {
                                long localRowID = cursor.next();
                                if (localRowID < lo) {
                                    break;
                                }
                                if (localRowID <= hi && searchLocalRowIDs.binarySearch(localRowID) >= 0) {
                                    result.add(Rows.toRowID(partition.getPartitionIndex(), localRowID));
                                }
                            }
                        }
                    }
                } else {
                    KVIndex.IndexCursor cursor = index.cachedCursor(symbolKey);
                    result.setCapacity((int) cursor.size());
                    while (cursor.hasNext()) {
                        long localRowID = cursor.next();
                        if (localRowID >= lo && localRowID <= hi) {
                            result.add(Rows.toRowID(partition.getPartitionIndex(), localRowID));
                        }
                    }
                }
            }
        }
    }
}
