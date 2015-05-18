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

package com.nfsdb.ql.impl;

import com.nfsdb.Journal;
import com.nfsdb.ql.SymFacade;
import com.nfsdb.storage.SymbolTable;

public class MasterSymFacade implements SymFacade {
    private Journal journal;

    @Override
    public SymbolTable getSymbolTable(int index) {
        // do not call journal.getSymbolTable() because it uses different indexing system
        return journal.getMetadata().getColumn(index).getSymbolTable();
    }

    @Override
    public SymbolTable getSymbolTable(String name) {
        return journal.getSymbolTable(name);
    }

    public void setJournal(Journal journal) {
        this.journal = journal;
    }
}
