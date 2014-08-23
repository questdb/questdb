package com.nfsdb.journal.lang.cst.impl;

import com.nfsdb.journal.Partition;
import com.nfsdb.journal.column.AbstractColumn;
import com.nfsdb.journal.column.VariableColumn;
import com.nfsdb.journal.exceptions.JournalRuntimeException;
import com.nfsdb.journal.lang.cst.Choice;
import com.nfsdb.journal.lang.cst.RowAcceptor;
import com.nfsdb.journal.lang.cst.RowFilter;

public class StringEqualsRowFilter implements RowFilter, RowAcceptor {
    private final String column;
    private final String value;
    private VariableColumn columnRef;

    public StringEqualsRowFilter(String column, String value) {
        this.column = column;
        this.value = value;
    }

    @Override
    public RowAcceptor acceptor(Partition partitionA, Partition partitionB) {
        AbstractColumn col = partitionA.getAbstractColumn(partitionA.getJournal().getMetadata().getColumnIndex(column));
        if (!(col instanceof VariableColumn)) {
            throw new JournalRuntimeException("Invalid column type");
        }
        columnRef = (VariableColumn) col;

        return this;
    }

    @Override
    public Choice accept(long localRowIDA, long localRowIDB) {
        return columnRef.equalsString(localRowIDA, value) ? Choice.PICK : Choice.SKIP;
    }
}
