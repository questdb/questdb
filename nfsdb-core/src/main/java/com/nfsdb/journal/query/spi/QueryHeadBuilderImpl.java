/*
 * Copyright (c) 2014-2015. Vlad Ilyushchenko
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

import com.nfsdb.journal.Journal;
import com.nfsdb.journal.Partition;
import com.nfsdb.journal.UnorderedResultSet;
import com.nfsdb.journal.UnorderedResultSetBuilder;
import com.nfsdb.journal.collections.IntArrayList;
import com.nfsdb.journal.collections.LongArrayList;
import com.nfsdb.journal.column.SymbolTable;
import com.nfsdb.journal.exceptions.JournalException;
import com.nfsdb.journal.index.KVIndex;
import com.nfsdb.journal.query.api.QueryHeadBuilder;
import com.nfsdb.journal.utils.Rows;
import org.joda.time.Interval;

import java.util.ArrayList;
import java.util.List;

public class QueryHeadBuilderImpl<T> implements QueryHeadBuilder<T> {

    private final Journal<T> journal;
    private final IntArrayList symbolKeys = new IntArrayList();
    private final List<String> filterSymbols = new ArrayList<>();
    private final IntArrayList filterSymbolKeys = new IntArrayList();
    private int symbolColumnIndex;
    private Interval interval;
    private long minRowID = -1L;
    private boolean strict = true;

    public QueryHeadBuilderImpl(Journal<T> journal) {
        this.journal = journal;
    }

    public void setSymbol(String symbol, String... values) {
        this.symbolColumnIndex = journal.getMetadata().getColumnIndex(symbol);
        SymbolTable symbolTable = journal.getSymbolTable(symbol);
        this.symbolKeys.resetQuick();
        this.symbolKeys.ensureCapacity(values == null || values.length == 0 ? symbolTable.size() : values.length);
        if (values == null || values.length == 0) {
            this.symbolKeys.ensureCapacity(symbolTable.size());
            for (int i = 0; i < symbolTable.size(); i++) {
                this.symbolKeys.add(i);
            }
        } else {
            this.symbolKeys.ensureCapacity(values.length);
            for (int i = 0; i < values.length; i++) {
                int key = symbolTable.getQuick(values[i]);
                if (key != SymbolTable.VALUE_NOT_FOUND) {
                    symbolKeys.add(key);
                }
            }
        }
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
    public QueryHeadBuilder<T> filter(String symbol, String value) {
        SymbolTable tab = journal.getSymbolTable(symbol);
        int key = tab.get(value);
        filterSymbols.add(symbol);
        filterSymbolKeys.add(key);
        return this;
    }

    @Override
    public void resetFilter() {
        filterSymbols.clear();
        filterSymbolKeys.resetQuick();
    }

    @Override
    public QueryHeadBuilder<T> strict(boolean strict) {
        this.strict = strict;
        return this;
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

        final IntArrayList symbolKeys = new IntArrayList(this.symbolKeys);

        return journal.iteratePartitionsDesc(
                new UnorderedResultSetBuilder<T>(interval) {
                    private final KVIndex filterKVIndexes[] = new KVIndex[filterSymbolKeys.size()];
                    private final LongArrayList filterSymbolRows[] = new LongArrayList[filterSymbolKeys.size()];
                    private IntArrayList keys = symbolKeys;
                    private IntArrayList remainingKeys = new IntArrayList(keys.size());

                    {
                        for (int i = 0; i < filterSymbolRows.length; i++) {
                            filterSymbolRows[i] = new LongArrayList();
                        }
                    }

                    @Override
                    public Accept accept(Partition<T> partition) throws JournalException {
                        super.accept(partition);
                        return keys.isEmpty() || partition.getPartitionIndex() < minPartitionIndex ? Accept.BREAK : Accept.CONTINUE;
                    }

                    @Override
                    public void read(long lo, long hi) throws JournalException {
                        KVIndex index = partition.getIndexForColumn(symbolColumnIndex);

                        boolean filterOk = true;
                        for (int i = 0; i < filterSymbols.size(); i++) {
                            filterKVIndexes[i] = partition.getIndexForColumn(filterSymbols.get(i));
                            int filterKey = filterSymbolKeys.getQuick(i);
                            if (filterKVIndexes[i].contains(filterKey)) {
                                filterSymbolRows[i].setCapacity(filterKVIndexes[i].getValueCount(filterKey));
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
                                KVIndex.IndexCursor cursor = index.cachedCursor(key);

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
                            IntArrayList temp = keys;
                            keys = remainingKeys;
                            remainingKeys = temp;
                            remainingKeys.resetQuick();
                        }

                    }
                }
        );
    }
}
