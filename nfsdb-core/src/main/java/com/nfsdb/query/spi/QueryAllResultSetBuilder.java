/*
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
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

import com.nfsdb.Partition;
import com.nfsdb.collections.IntList;
import com.nfsdb.collections.LongList;
import com.nfsdb.collections.ObjList;
import com.nfsdb.exceptions.JournalException;
import com.nfsdb.query.UnorderedResultSetBuilder;
import com.nfsdb.storage.IndexCursor;
import com.nfsdb.storage.KVIndex;
import com.nfsdb.utils.Interval;
import com.nfsdb.utils.Rows;

public class QueryAllResultSetBuilder<T> extends UnorderedResultSetBuilder<T> {
    private final IntList symbolKeys;
    private final ObjList<String> filterSymbols;
    private final IntList filterSymbolKeys;
    final private String symbol;
    private KVIndex index;
    private KVIndex[] searchIndices;

    public QueryAllResultSetBuilder(Interval interval, String symbol, IntList symbolKeys, ObjList<String> filterSymbols, IntList filterSymbolKeys) {
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
            for (int i = 0, sz = symbolKeys.size(); i < sz; i++) {
                if (index.contains(symbolKeys.getQuick(i))) {
                    int n = filterSymbols.size();
                    searchIndices = new KVIndex[n];
                    for (int k = 0; k < n; k++) {
                        searchIndices[k] = partition.getIndexForColumn(filterSymbols.getQuick(k));
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
        for (int i = 0, sz = symbolKeys.size(); i < sz; i++) {
            int symbolKey = symbolKeys.getQuick(i);
            if (index.contains(symbolKey)) {
                if (searchIndices.length > 0) {
                    for (int k = 0; k < searchIndices.length; k++) {
                        if (searchIndices[k].contains(filterSymbolKeys.getQuick(k))) {
                            LongList searchLocalRowIDs = searchIndices[k].getValues(filterSymbolKeys.getQuick(k));

                            IndexCursor cursor = index.cursor(symbolKey);
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
                    IndexCursor cursor = index.cursor(symbolKey);
                    result.ensureCapacity((int) cursor.size());
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
