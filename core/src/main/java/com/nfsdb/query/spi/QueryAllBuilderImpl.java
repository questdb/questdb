/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2016. The NFSdb project and its contributors.
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
 ******************************************************************************/

package com.nfsdb.query.spi;

import com.nfsdb.Journal;
import com.nfsdb.ex.JournalException;
import com.nfsdb.misc.Interval;
import com.nfsdb.query.UnorderedResultSet;
import com.nfsdb.query.api.QueryAllBuilder;
import com.nfsdb.std.IntList;
import com.nfsdb.std.ObjList;
import com.nfsdb.store.SymbolTable;

public class QueryAllBuilderImpl<T> implements QueryAllBuilder<T> {

    private final Journal<T> journal;
    private final IntList symbolKeys = new IntList();
    private final ObjList<String> filterSymbols = new ObjList<>();
    private final IntList filterSymbolKeys = new IntList();
    private String symbol;
    private Interval interval;

    public QueryAllBuilderImpl(Journal<T> journal) {
        this.journal = journal;
    }

    @Override
    public UnorderedResultSet<T> asResultSet() throws JournalException {
        return journal.iteratePartitionsDesc(new QueryAllResultSetBuilder<T>(interval, symbol, symbolKeys, filterSymbols, filterSymbolKeys));
    }

    @Override
    public QueryAllBuilder<T> filter(String symbol, String value) {
        SymbolTable tab = journal.getSymbolTable(symbol);
        int key = tab.get(value);
        filterSymbols.add(symbol);
        filterSymbolKeys.add(key);
        return this;
    }

    @Override
    public void resetFilter() {
        filterSymbols.clear();
        filterSymbolKeys.clear();
    }

    @Override
    public QueryAllBuilder<T> slice(Interval interval) {
        setInterval(interval);
        return this;
    }

    public void setSymbol(String symbol, String... values) {
        this.symbol = symbol;
        SymbolTable symbolTable = journal.getSymbolTable(symbol);
        this.symbolKeys.clear();
        for (int i = 0; i < values.length; i++) {
            int key = symbolTable.getQuick(values[i]);
            if (key != SymbolTable.VALUE_NOT_FOUND) {
                symbolKeys.add(key);
            }
        }
    }

    private void setInterval(Interval interval) {
        this.interval = interval;
    }
}
