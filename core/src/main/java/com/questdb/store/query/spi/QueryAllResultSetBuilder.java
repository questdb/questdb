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

import com.questdb.std.IntList;
import com.questdb.std.LongList;
import com.questdb.std.ObjList;
import com.questdb.std.Rows;
import com.questdb.std.ex.JournalException;
import com.questdb.store.IndexCursor;
import com.questdb.store.Interval;
import com.questdb.store.KVIndex;
import com.questdb.store.Partition;
import com.questdb.store.query.UnorderedResultSetBuilder;

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
