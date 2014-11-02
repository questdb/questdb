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

package com.nfsdb.journal.lang.cst.impl.dsrc;

import com.nfsdb.journal.Journal;
import com.nfsdb.journal.collections.AbstractImmutableIterator;
import com.nfsdb.journal.column.FixedColumn;
import com.nfsdb.journal.column.SymbolTable;
import com.nfsdb.journal.column.VariableColumn;
import com.nfsdb.journal.lang.cst.DataItem;
import com.nfsdb.journal.lang.cst.JournalSource;

public class DataRowSourceImpl extends AbstractImmutableIterator<DataRow> implements DataRowSource, DataRow {
    private final JournalSource source;
    private final Journal journal;
    private DataItem current;

    public DataRowSourceImpl(JournalSource source) {
        this.source = source;
        this.journal = source.getJournal();
    }

    @Override
    public boolean hasNext() {
        return source.hasNext();
    }

    @Override
    public DataRow next() {
        current = source.next();
        return this;
    }

    @Override
    public Journal getJournal() {
        return journal;
    }

    @Override
    public int getColumnIndex(String column) {
        return journal.getMetadata().getColumnIndex(column);
    }

    @Override
    public byte get(String column) {
        return get(journal.getMetadata().getColumnIndex(column));
    }

    @Override
    public byte get(int col) {
        return ((FixedColumn) current.partition.getAbstractColumn(col)).getByte(current.rowid);
    }

    @Override
    public int getInt(String column) {
        return getInt(journal.getMetadata().getColumnIndex(column));
    }

    @Override
    public int getInt(int col) {
        return ((FixedColumn) current.partition.getAbstractColumn(col)).getInt(current.rowid);
    }

    @Override
    public long getLong(String column) {
        return getLong(journal.getMetadata().getColumnIndex(column));
    }

    @Override
    public long getLong(int col) {
        return ((FixedColumn) current.partition.getAbstractColumn(col)).getLong(current.rowid);
    }

    @Override
    public double getDouble(String column) {
        return getDouble(journal.getMetadata().getColumnIndex(column));
    }

    @Override
    public double getDouble(int col) {
        return ((FixedColumn) current.partition.getAbstractColumn(col)).getDouble(current.rowid);
    }

    @Override
    public String getStr(String column) {
        return getStr(journal.getMetadata().getColumnIndex(column));
    }

    @Override
    public String getStr(int col) {
        return ((VariableColumn) current.partition.getAbstractColumn(col)).getString(current.rowid);
    }

    @Override
    public String getSym(String column) {
        return getSym(journal.getMetadata().getColumnIndex(column));
    }

    @Override
    public String getSym(int col) {
        int key = getInt(col);
        return key == SymbolTable.VALUE_IS_NULL ? null : journal.getColumnMetadata(col).symbolTable.value(key);
    }

    @Override
    public boolean getBool(String column) {
        return getBool(journal.getMetadata().getColumnIndex(column));
    }

    @Override
    public boolean getBool(int col) {
        return ((FixedColumn) current.partition.getAbstractColumn(col)).getBool(current.rowid);
    }

    @Override
    public void reset() {
        source.reset();
    }
}
