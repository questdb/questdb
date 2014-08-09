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

package com.nfsdb.journal.index;

import com.nfsdb.journal.Journal;
import com.nfsdb.journal.Partition;
import com.nfsdb.journal.exceptions.JournalException;
import com.nfsdb.journal.index.experimental.FilteredCursor;
import com.nfsdb.journal.index.experimental.filter.StringEqualsFilter;
import com.nfsdb.journal.utils.Checksum;

public class StringIndexCursor extends FilteredCursor<IndexCursor, StringEqualsFilter> {

    private String columnName;
    private String value;

    public StringIndexCursor() {
        super(new IndexCursor(), new StringEqualsFilter());
    }

    @Override
    public void configure(Partition partition) throws JournalException {
        Journal j = partition.getJournal();
        int hashCapacity = j.getColumnMetadata(j.getMetadata().getColumnIndex(columnName)).meta.distinctCountHint;
        delegate.with(columnName, Checksum.hash(value, hashCapacity));
        super.configure(partition);
    }

    public StringIndexCursor with(String columnName, String value) {
        this.columnName = columnName;
        this.value = value;
        filter.withColumn(columnName);
        filter.withValue(value);
        return this;
    }
}
