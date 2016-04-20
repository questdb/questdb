/*******************************************************************************
 * ___                  _   ____  ____
 * / _ \ _   _  ___  ___| |_|  _ \| __ )
 * | | | | | | |/ _ \/ __| __| | | |  _ \
 * | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 * \__\_\\__,_|\___||___/\__|____/|____/
 * <p>
 * Copyright (C) 2014-2016 Appsicle
 * <p>
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 * <p>
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * <p>
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * <p>
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 ******************************************************************************/

package com.questdb.query.spi;

import com.questdb.Partition;
import com.questdb.ex.JournalException;
import com.questdb.misc.Interval;
import com.questdb.misc.Rows;
import com.questdb.query.UnorderedResultSetBuilder;
import com.questdb.std.IntList;
import com.questdb.std.LongList;
import com.questdb.std.ObjList;
import com.questdb.store.IndexCursor;
import com.questdb.store.KVIndex;

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
