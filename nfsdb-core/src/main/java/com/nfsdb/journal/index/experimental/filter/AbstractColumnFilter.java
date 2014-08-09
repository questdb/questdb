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

package com.nfsdb.journal.index.experimental.filter;

import com.nfsdb.journal.Partition;
import com.nfsdb.journal.column.AbstractColumn;
import com.nfsdb.journal.index.experimental.CursorFilter;

public abstract class AbstractColumnFilter implements CursorFilter {

    protected AbstractColumn column;
    protected String columnName;
    private int columnIndex = -1;

    public AbstractColumnFilter withColumn(String columnName) {
        this.columnName = columnName;
        this.columnIndex = -1;
        return this;
    }

    public void configure(Partition partition) {
        if (this.columnIndex == -1) {
            this.columnIndex = partition.getJournal().getMetadata().getColumnIndex(columnName);
        }
        this.column = partition.getAbstractColumn(columnIndex);
    }
}
