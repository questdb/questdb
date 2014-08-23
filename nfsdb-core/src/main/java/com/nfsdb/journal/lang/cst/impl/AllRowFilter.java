package com.nfsdb.journal.lang.cst.impl;

import com.nfsdb.journal.Partition;
import com.nfsdb.journal.lang.cst.Choice;
import com.nfsdb.journal.lang.cst.RowAcceptor;
import com.nfsdb.journal.lang.cst.RowFilter;

public class AllRowFilter implements RowFilter, RowAcceptor {

    private final RowFilter[] filters;
    private final RowAcceptor[] acceptors;

    public AllRowFilter(RowFilter[] filters) {
        this.filters = filters;
        this.acceptors = new RowAcceptor[filters.length];
    }

    @Override
    public RowAcceptor acceptor(Partition partitionA, Partition partitionB) {
        for (int i = 0; i < filters.length; i++) {
            RowFilter filter = filters[i];
            acceptors[i] = filter.acceptor(partitionA, partitionB);

        }
        return this;
    }

    @Override
    public Choice accept(long localRowIDA, long localRowIDB) {
        for (int i = 0; i < acceptors.length; i++) {
            RowAcceptor acceptor = acceptors[i];
            Choice choice = acceptor.accept(localRowIDA, localRowIDB);
            if (choice != Choice.PICK) {
                return choice;
            }
        }
        return Choice.PICK;
    }
}
