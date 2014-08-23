package com.nfsdb.journal.lang.cst.impl;

import com.nfsdb.journal.Partition;
import com.nfsdb.journal.column.AbstractColumn;
import com.nfsdb.journal.column.FixedColumn;
import com.nfsdb.journal.exceptions.JournalRuntimeException;
import com.nfsdb.journal.lang.cst.Choice;
import com.nfsdb.journal.lang.cst.RowAcceptor;
import com.nfsdb.journal.lang.cst.RowFilter;

public class GreaterThanRowFilter implements RowFilter, RowAcceptor {
    private final String column;
    private final double value;
    private FixedColumn columnRef;

    public GreaterThanRowFilter(String column, double value) {
        this.column = column;
        this.value = value;
    }

    @Override
    public RowAcceptor acceptor(Partition partitionA, Partition partitionB) {
        AbstractColumn col = partitionA.getAbstractColumn(partitionA.getJournal().getMetadata().getColumnIndex(column));
        if (!(col instanceof FixedColumn)) {
            throw new JournalRuntimeException("Invalid column type");
        }
        columnRef = (FixedColumn) col;

        return this;
    }

    @Override
    public Choice accept(long localRowIDA, long localRowIDB) {
        return columnRef.getDouble(localRowIDA) > value ? Choice.PICK : Choice.SKIP;
    }
}
