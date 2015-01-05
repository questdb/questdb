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

import com.nfsdb.lang.cst.Choice;
import com.nfsdb.lang.cst.PartitionSlice;
import com.nfsdb.lang.cst.RowAcceptor;
import com.nfsdb.lang.cst.RowFilter;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class AllRowFilter implements RowFilter, RowAcceptor {

    private final RowFilter[] filters;
    private final RowAcceptor[] acceptors;

    @SuppressFBWarnings(justification = "By ref parameter to avoid paranoid array copying")
    public AllRowFilter(RowFilter[] filters) {
        this.filters = filters;
        this.acceptors = new RowAcceptor[filters.length];
    }

    @Override
    public RowAcceptor acceptor(PartitionSlice a) {
        for (int i = 0; i < filters.length; i++) {
            RowFilter filter = filters[i];
            acceptors[i] = filter.acceptor(a);

        }
        return this;
    }

    @Override
    public Choice accept(long localRowID) {
        for (int i = 0; i < acceptors.length; i++) {
            RowAcceptor acceptor = acceptors[i];
            Choice choice = acceptor.accept(localRowID);
            if (choice != Choice.PICK) {
                return choice;
            }
        }
        return Choice.PICK;
    }
}
