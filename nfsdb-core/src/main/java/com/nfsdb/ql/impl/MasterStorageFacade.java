/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
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
 ******************************************************************************/

package com.nfsdb.ql.impl;

import com.nfsdb.Journal;
import com.nfsdb.factory.JournalReaderFactory;
import com.nfsdb.ql.StorageFacade;
import com.nfsdb.storage.SymbolTable;

public class MasterStorageFacade implements StorageFacade {
    private Journal journal;
    private JournalReaderFactory factory;

    @Override
    public JournalReaderFactory getFactory() {
        return factory;
    }

    @Override
    public SymbolTable getSymbolTable(int index) {
        // do not call journal.getSymbolTable() because it uses different indexing system
        return journal.getMetadata().getColumn(index).getSymbolTable();
    }

    @Override
    public SymbolTable getSymbolTable(String name) {
        return journal.getSymbolTable(name);
    }

    public void setFactory(JournalReaderFactory factory) {
        this.factory = factory;
    }

    public void setJournal(Journal journal) {
        this.journal = journal;
    }
}
