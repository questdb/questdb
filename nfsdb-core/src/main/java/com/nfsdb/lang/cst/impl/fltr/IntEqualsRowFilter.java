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
import com.nfsdb.exceptions.JournalException;
import com.nfsdb.exceptions.JournalRuntimeException;
import com.nfsdb.lang.cst.*;
import com.nfsdb.lang.cst.impl.ref.StringRef;

public class IntEqualsRowFilter implements RowFilter, RowAcceptor {
    private final StringRef column;
    private final IntVariableSource variableSource;
    private FixedColumn columnRef;
    private IntVariable var;

    public IntEqualsRowFilter(StringRef column, IntVariableSource variableSource) {
        this.column = column;
        this.variableSource = variableSource;
    }

    @Override
    public RowAcceptor acceptor(PartitionSlice slice) {
        try {
            slice.partition.open();
            AbstractColumn col = slice.partition.getAbstractColumn(slice.partition.getJournal().getMetadata().getColumnIndex(column.value));
            if (!(col instanceof FixedColumn)) {
                throw new JournalRuntimeException("Invalid column type");
            }
            columnRef = (FixedColumn) col;
            var = variableSource.getVariable(slice);
            return this;
        } catch (JournalException e) {
            throw new JournalRuntimeException(e);
        }
    }

    @Override
    public Choice accept(long localRowID) {
        return columnRef.getInt(localRowID) == var.getValue() ? Choice.PICK : Choice.SKIP;
    }
}
