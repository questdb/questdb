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

package com.nfsdb.lang.cst.impl.qry;

import com.nfsdb.Journal;
import com.nfsdb.collections.AbstractImmutableIterator;
import com.nfsdb.column.ColumnType;
import com.nfsdb.column.SymbolTable;
import com.nfsdb.factory.configuration.JournalMetadata;
import com.nfsdb.factory.configuration.RecordColumnMetadata;
import com.nfsdb.lang.cst.JournalRecordSource;
import com.nfsdb.lang.cst.RecordMetadata;
import com.nfsdb.storage.ColumnType;
import com.nfsdb.storage.SymbolTable;

public abstract class AbstractJournalSource extends AbstractImmutableIterator<JournalRecord> implements JournalRecordSource, RecordMetadata {
    private final JournalMetadata metadata;

    protected AbstractJournalSource(JournalMetadata metadata) {
        this.metadata = metadata;
    }

    @Override
    public int getColumnCount() {
        return metadata.getColumnCount();
    }

    @Override
    public int getColumnIndex(CharSequence name) {
        return metadata.getColumnIndex(name);
    }

    @Override
    public String getColumnName(int index) {
        return metadata.getColumnMetadata(index).name;
    }

    @Override
    public ColumnType getColumnType(int index) {
        return metadata.getColumnMetadata(index).type;
    }

    @Override
    public RecordColumnMetadata getColumn(int index) {
        return metadata.getColumnMetadata(index);
    }
    
    @Override
    public SymbolTable getSymbolTable(int index) {
        Journal j = getJournal();
        return j.getSymbolTable(j.getMetadata().getColumnMetadata(index).name);
    }

    @Override
    public RecordColumnMetadata getColumn(CharSequence name) {
        return metadata.getColumnMetadata(name);
    }
}
