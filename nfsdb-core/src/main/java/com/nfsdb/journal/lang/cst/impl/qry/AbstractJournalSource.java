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

package com.nfsdb.journal.lang.cst.impl.qry;

import com.nfsdb.journal.collections.AbstractImmutableIterator;
import com.nfsdb.journal.column.ColumnType;

public abstract class AbstractJournalSource extends AbstractImmutableIterator<JournalRecord> implements JournalRecordSource, RecordMetadata {

    @Override
    public RecordMetadata nextMetadata() {
        return null;
    }

    @Override
    public int getColumnCount() {
        return getJournal().getMetadata().getColumnCount();
    }

    @Override
    public ColumnType getColumnType(int index) {
        return getJournal().getMetadata().getColumnMetadata(index).type;
    }

    @Override
    public int getColumnIndex(CharSequence name) {
        return getJournal().getMetadata().getColumnIndex(name);
    }

    @Override
    public RecordMetadata getMetadata() {
        return this;
    }
}
