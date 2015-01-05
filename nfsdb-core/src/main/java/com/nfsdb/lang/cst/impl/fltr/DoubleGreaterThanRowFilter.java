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

package com.nfsdb.lang.cst.impl.fltr;

import com.nfsdb.column.AbstractColumn;
import com.nfsdb.column.FixedColumn;
import com.nfsdb.exceptions.JournalRuntimeException;
import com.nfsdb.lang.cst.Choice;
import com.nfsdb.lang.cst.PartitionSlice;
import com.nfsdb.lang.cst.RowAcceptor;
import com.nfsdb.lang.cst.RowFilter;

public class DoubleGreaterThanRowFilter implements RowFilter, RowAcceptor {
    private final String column;
    private final double value;
    private FixedColumn columnRef;

    public DoubleGreaterThanRowFilter(String column, double value) {
        this.column = column;
        this.value = value;
    }

    @Override
    public RowAcceptor acceptor(PartitionSlice a) {
        AbstractColumn col = a.partition.getAbstractColumn(a.partition.getJournal().getMetadata().getColumnIndex(column));
        if (!(col instanceof FixedColumn)) {
            throw new JournalRuntimeException("Invalid column type");
        }
        columnRef = (FixedColumn) col;

        return this;
    }

    @Override
    public Choice accept(long localRowID) {
        return columnRef.getDouble(localRowID) > value ? Choice.PICK : Choice.SKIP;
    }
}
