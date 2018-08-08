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
import com.questdb.store.*;
import com.questdb.store.query.UnorderedResultSet;
import com.questdb.store.query.UnorderedResultSetBuilder;
import com.questdb.store.query.api.QueryHeadBuilder;

public class QueryHeadBuilderImpl<T> implements QueryHeadBuilder<T> {

    private final Journal<T> journal;
    private final IntList symbolKeys = new IntList();
    private final IntList zone1Keys = new IntList();
    private final IntList zone2Keys = new IntList();
    private final ObjList<String> filterSymbols = new ObjList<>();
    private final IntList filterSymbolKeys = new IntList();
    private int symbolColumnIndex;
    private Interval interval;
    private long minRowID = -1L;
    private boolean strict = true;

    public QueryHeadBuilderImpl(Journal<T> journal) {
        this.journal = journal;
    }

    public UnorderedResultSet<T> asResultSet() throws JournalException {
        final int minPartitionIndex;
        final long minLocalRowID;

        if (minRowID == -1L) {
            minPartitionIndex = 0;
            minLocalRowID = -1L;
        } else {
            minPartitionIndex = Rows.toPartitionIndex(minRowID);
            minLocalRowID = Rows.toLocalRowID(minRowID);
        }

        zone1Keys.clear(symbolKeys.size());
        zone2Keys.clear(symbolKeys.size());
        zone1Keys.addAll(symbolKeys);

        //noinspection ConstantConditions
        return journal.iteratePartitionsDesc(
                new UnorderedResultSetBuilder<T>(interval) {
                    private final KVIndex filterKVIndexes[] = new KVIndex[filterSymbolKeys.size()];
                    private final LongList filterSymbolRows[] = new LongList[filterSymbolKeys.size()];
                    private IntList keys = zone1Keys;
                    private IntList remainingKeys = zone2Keys;

                    @Override
                    public Accept accept(Partition<T> partition) throws JournalException {
                        super.accept(partition);
                        return keys.size() == 0 || partition.getPartitionIndex() < minPartitionIndex ? Accept.BREAK : Accept.CONTINUE;
                    }

                    @Override
                    public void read(long lo, long hi) throws JournalException {
                        KVIndex index = partition.getIndexForColumn(symbolColumnIndex);

                        boolean filterOk = true;
                        for (int i = 0; i < filterSymbols.size(); i++) {
                            filterKVIndexes[i] = partition.getIndexForColumn(filterSymbols.getQuick(i));
                            int filterKey = filterSymbolKeys.getQuick(i);
                            if (filterKVIndexes[i].contains(filterKey)) {
                                filterSymbolRows[i].ensureCapacity(filterKVIndexes[i].getValueCount(filterKey));
                                filterKVIndexes[i].getValues(filterKey, filterSymbolRows[i]);
                            } else {
                                filterOk = false;
                                break;
                            }

                        }

                        if (filterOk) {
                            for (int k = 0; k < keys.size(); k++) {
                                int key = keys.getQuick(k);
                                boolean found = false;
                                IndexCursor cursor = index.cursor(key);

                                NEXT_KEY:
                                while (cursor.hasNext()) {
                                    long localRowID = cursor.next();
                                    if (localRowID <= hi && localRowID >= lo && (partition.getPartitionIndex() > minPartitionIndex || localRowID > minLocalRowID)) {

                                        boolean matches = true;
                                        for (int i = 0; i < filterSymbolRows.length; i++) {
                                            if (filterSymbolRows[i].binarySearch(localRowID) < 0) {
                                                matches = false;
                                                if (strict) {
                                                    found = true;
                                                    break NEXT_KEY;
                                                } else {
                                                    break;
                                                }
                                            }
                                        }

                                        if (matches) {
                                            result.add(Rows.toRowID(partition.getPartitionIndex(), localRowID));
                                            found = true;
                                            break;
                                        }

                                    } else if (localRowID < lo || (partition.getPartitionIndex() <= minPartitionIndex && localRowID <= minLocalRowID)) {
                                        // localRowID is only going to get lower, so fail fast
                                        found = true;
                                        break;
                                    }
                                }

                                if (!found) {
                                    remainingKeys.add(key);
                                }
                            }
                            IntList temp = keys;
                            keys = remainingKeys;
                            remainingKeys = temp;
                            remainingKeys.clear();
                        }
                    }

                    {
                        for (int i = 0; i < filterSymbolRows.length; i++) {
                            filterSymbolRows[i] = new LongList();
                        }
                    }
                }
        );
    }

    @Override
    public QueryHeadBuilder<T> filter(String symbol, String value) {
        MMappedSymbolTable tab = journal.getSymbolTable(symbol);
        int key = tab.get(value);
        filterSymbols.add(symbol);
        filterSymbolKeys.add(key);
        return this;
    }

    @Override
    public QueryHeadBuilder<T> limit(Interval interval) {
        this.interval = interval;
        this.minRowID = -1L;
        return this;
    }

    @Override
    public QueryHeadBuilder<T> limit(long minRowID) {
        this.minRowID = minRowID;
        this.interval = null;
        return this;
    }

    @Override
    public void resetFilter() {
        filterSymbols.clear();
        filterSymbolKeys.clear();
    }

    @Override
    public QueryHeadBuilder<T> strict(boolean strict) {
        this.strict = strict;
        return this;
    }

    public void setSymbol(String symbol, String... values) {
        this.symbolColumnIndex = journal.getMetadata().getColumnIndex(symbol);
        MMappedSymbolTable symbolTable = journal.getSymbolTable(symbol);
        this.symbolKeys.clear(values == null || values.length == 0 ? symbolTable.size() : values.length);
        if (values == null || values.length == 0) {
            int sz = symbolTable.size();
            for (int i = 0; i < sz; i++) {
                this.symbolKeys.add(i);
            }
        } else {
            for (int i = 0; i < values.length; i++) {
                int key = symbolTable.getQuick(values[i]);
                if (key != SymbolTable.VALUE_NOT_FOUND) {
                    symbolKeys.add(key);
                }
            }
        }
    }
}
